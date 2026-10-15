import { IsolationLevel, LockMode } from '@mikro-orm/core';
import type { MikroORM } from '@mikro-orm/postgresql';
import { OutboxMessage } from '../../domain/messaging/outbox-message';
import type { SqsGateway } from '../messaging/sqs/sqs-gateway';
import type { Logger } from '../observability/logger';
import type { AppMetrics } from '../observability/metrics';
import { OutboxRecord } from '../persistence/schemas';

export interface OutboxPublisherOptions {
  batchSize: number;
  /** Teto de cada instrução da transação de publicação (SET LOCAL statement_timeout). */
  statementTimeoutMs?: number;
  /**
   * Injeção de falha para testes de crash com processo real: beforeSend roda com o lote já reivindicado
   * (linhas travadas) e ANTES do SendMessage; afterSend roda depois do SendMessage e ANTES do commit.
   */
  hooks?: { beforeSend?(eventIds: string[]): Promise<void> | void; afterSend?(eventIds: string[]): Promise<void> | void };
}

/**
 * Publica eventos confirmados da outbox. Seguro com N publishers concorrentes:
 *  - SELECT ... FOR UPDATE SKIP LOCKED (LockMode.PESSIMISTIC_PARTIAL_WRITE): cada publisher
 *    reivindica um lote DIFERENTE; nenhum espera o outro nem pega a mesma linha;
 *  - publish e marcação `published_at` na MESMA transação: se o processo morrer depois do
 *    SendMessage e antes do commit, o lock cai, a linha continua pendente e outro publisher
 *    republica. Duplicata possível, perda impossível: at-least-once, consumidor deduplica por
 *    eventId (que também é o MessageDeduplicationId da FIFO).
 * Trade-off (ARCHITECTURE.md): a transação/conexão fica aberta durante o I/O com o SQS.
 */
export class OutboxPublisher {
  constructor(
    private readonly orm: MikroORM,
    private readonly gateway: SqsGateway,
    private readonly metrics: AppMetrics,
    private readonly logger: Logger,
    private readonly options: OutboxPublisherOptions,
  ) {}

  async publishOnce(): Promise<{ published: number; failed: number }> {
    return this.orm.em.fork().transactional(
      async (em) => {
        await em.execute(`SET LOCAL statement_timeout = ${Math.floor(this.options.statementTimeoutMs ?? 15_000)}`);
        const now = new Date();
        const rows = await em.find(
          OutboxRecord,
          { publishedAt: null, nextAttemptAt: { $lte: now } },
          {
            orderBy: { nextAttemptAt: 'asc', occurredAt: 'asc' },
            limit: this.options.batchSize,
            lockMode: LockMode.PESSIMISTIC_PARTIAL_WRITE,
            disableIdentityMap: true,
          },
        );
        if (rows.length === 0) return { published: 0, failed: 0 };
        await this.options.hooks?.beforeSend?.(rows.map((r) => r.id));

        let failures: Map<string, string>;
        try {
          failures = await this.gateway.publishEvents(
            rows.map((r) => ({ id: r.id, groupId: r.aggregateId, eventType: r.eventType, body: JSON.stringify(r.payload) })),
          );
        } catch (error) {
          // SQS fora: o lote inteiro é reagendado com backoff (nunca descartado, decisão G).
          failures = new Map(rows.map((r) => [r.id, (error as Error).message ?? 'publish failed']));
        }
        await this.options.hooks?.afterSend?.(rows.map((r) => r.id));

        const published = rows.filter((r) => !failures.has(r.id)).map((r) => r.id);
        if (published.length > 0) {
          await em.nativeUpdate(OutboxRecord, { id: { $in: published } }, { publishedAt: now });
        }
        for (const row of rows.filter((r) => failures.has(r.id))) {
          const message = OutboxMessage.rehydrate({
            ...row,
            payload: row.payload,
            nextAttemptAt: row.nextAttemptAt,
            publishedAt: row.publishedAt ?? undefined,
            lastError: row.lastError ?? undefined,
          });
          message.scheduleRetry(now, failures.get(row.id));
          await em.nativeUpdate(
            OutboxRecord,
            { id: row.id },
            { attempts: message.attempts, nextAttemptAt: message.nextAttemptAt!, lastError: message.lastError ?? null },
          );
        }
        this.metrics.outboxPublished.inc(published.length);
        if (failures.size > 0) {
          this.metrics.outboxPublishFailures.inc(failures.size);
          this.logger.warn({ msg: 'outbox publish failed; rescheduled', count: failures.size });
        }
        return { published: published.length, failed: failures.size };
      },
      { isolationLevel: IsolationLevel.READ_COMMITTED },
    );
  }

  /** outbox lag = idade do evento pendente mais antigo, medida pelo relógio do banco. */
  async refreshLag(): Promise<{ pending: number; lagSeconds: number }> {
    const [row] = await this.orm.em
      .fork()
      .execute<{ pending: string; lag: string | null }[]>(
        `SELECT count(*)::text AS pending, EXTRACT(EPOCH FROM now() - min(occurred_at))::text AS lag
           FROM outbox_messages WHERE published_at IS NULL`,
      );
    const pending = Number(row?.pending ?? 0);
    const lagSeconds = row?.lag ? Math.max(0, Number(row.lag)) : 0;
    this.metrics.outboxPending.set(pending);
    this.metrics.outboxLag.set(lagSeconds);
    return { pending, lagSeconds };
  }
}
