/**
 * Transactional outbox contra Postgres e LocalStack REAIS.
 * O "contador" de envios é um spy que DELEGA ao gateway real (o SQS recebe de verdade):
 * serve só para provar quantas vezes cada eventId foi enviado.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { CreateQueueCommand, DeleteQueueCommand, type SQSClient } from '@aws-sdk/client-sqs';
import type { MikroORM } from '@mikro-orm/postgresql';
import { createLogger } from '../../src/infrastructure/observability/logger';
import { AppMetrics } from '../../src/infrastructure/observability/metrics';
import type { OutgoingEvent, SqsGateway } from '../../src/infrastructure/messaging/sqs/sqs-gateway';
import { OutboxPublisher } from '../../src/infrastructure/workers/outbox-publisher';
import { type AppContext, createAppContext, inParallel, openTestWallet, wagerCommand } from '../support/app-context';
import { ledgerInvariantAfterEach } from '../support/invariants';
import { createTestOrm } from '../support/db';
import { createTestQueues, drain } from '../support/sqs';

const logger = createLogger();
logger.level = 'silent';
let app: AppContext;
let gateway: SqsGateway;
let client: SQSClient;
let eventsQueueName: string;
const touch = ledgerInvariantAfterEach(() => app.orm);

beforeAll(async () => {
  app = await createAppContext();
  const queues = await createTestQueues('outbox');
  ({ gateway, client } = queues);
  eventsQueueName = queues.names.events;
  // A outbox acumula eventos dos outros testes (nenhum publisher roda neles). Escoa o backlog
  // antes, para que cada teste meça só os próprios eventos.
  const publisher = new OutboxPublisher(app.orm, gateway, new AppMetrics(), logger, { batchSize: 200 });
  while ((await publisher.publishOnce()).published > 0);
  await drain(client, gateway.urls.events, 1_000);
}, 180_000); // backlog pode ter milhares de eventos dos testes de concorrência
afterAll(async () => {
  await app.close();
});

function countingGateway(real: SqsGateway, sent: Map<string, number>): SqsGateway {
  return {
    urls: real.urls,
    publishEvents: async (events: OutgoingEvent[]) => {
      for (const e of events) sent.set(e.id, (sent.get(e.id) ?? 0) + 1);
      return real.publishEvents(events);
    },
  } as unknown as SqsGateway;
}

/** Gera eventos confirmados (via use case real) e devolve os eventIds. */
async function produceEvents(bets: number): Promise<{ walletId: string; eventIds: string[] }> {
  const w = await openTestWallet(app, '1000.00');
  touch(w.id);
  for (let i = 0; i < bets; i++) await app.processTransaction.execute(wagerCommand(w, { money: { amount: '1.00', currency: 'BRL' } }));
  const rows = await app.orm.em
    .fork()
    .execute<{ id: string }[]>(
      `SELECT id FROM outbox_messages WHERE aggregate_id = ? OR aggregate_id IN (SELECT id FROM wager_transactions WHERE wallet_id = ?)`,
      [w.id, w.id],
    );
  return { walletId: w.id, eventIds: rows.map((r) => r.id) };
}

async function unpublished(orm: MikroORM, ids: string[]): Promise<number> {
  const [row] = await orm.em
    .fork()
    .execute<{ n: string }[]>(`SELECT count(*)::text AS n FROM outbox_messages WHERE id = ANY(?::uuid[]) AND published_at IS NULL`, [
      `{${ids.join(',')}}`,
    ]);
  return Number(row?.n ?? 0);
}

async function publishUntilDone(publisher: OutboxPublisher, orm: MikroORM, ids: string[], timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs;
  while ((await unpublished(orm, ids)) > 0) {
    if (Date.now() > deadline) throw new Error('outbox not drained in time');
    const { published } = await publisher.publishOnce();
    if (published === 0) await Bun.sleep(50);
  }
}

describe('outbox', () => {
  test('evento confirmado é publicado depois do commit, com envelope versionado e MoneyProps', async () => {
    const { eventIds } = await produceEvents(1); // OPENING (2 eventos) + BET (2 eventos)
    expect(eventIds).toHaveLength(4);
    const publisher = new OutboxPublisher(app.orm, gateway, new AppMetrics(), logger, { batchSize: 50 });
    await publishUntilDone(publisher, app.orm, eventIds);

    const received = (await drain(client, gateway.urls.events)).map((m) => JSON.parse(m.body));
    const ours = received.filter((e) => eventIds.includes(e.eventId));
    expect(ours).toHaveLength(4);
    const changed = ours.find((e) => e.eventType === 'WalletBalanceChanged' && e.data.direction === 'DEBIT');
    expect(changed).toMatchObject({
      version: 1,
      aggregateType: 'Wallet',
      data: { money: { amount: '1.00', currency: 'BRL' }, balanceBefore: { amount: '1000.00', currency: 'BRL' }, walletVersion: 2 },
    });
  });

  test('DOIS publishers concorrentes (pools próprios) sobre a mesma outbox: cada evento enviado exatamente uma vez', async () => {
    const { eventIds } = await produceEvents(25); // 2 + 50 eventos
    const [ormA, ormB] = await Promise.all([createTestOrm(3), createTestOrm(3)]);
    const sent = new Map<string, number>();
    const publishers = [ormA, ormB].map(
      (orm) => new OutboxPublisher(orm, countingGateway(gateway, sent), new AppMetrics(), logger, { batchSize: 5 }),
    );
    const perPublisher = [0, 0];
    await inParallel(2, async (i) => {
      while ((await unpublished(app.orm, eventIds)) > 0) {
        const { published } = await publishers[i]!.publishOnce();
        perPublisher[i]! += published;
        if (published === 0) await Bun.sleep(20);
      }
    });
    await Promise.all([ormA.close(true), ormB.close(true)]);

    for (const id of eventIds) expect(sent.get(id)).toBe(1); // SKIP LOCKED: nenhum lote reivindicado duas vezes
    expect(await unpublished(app.orm, eventIds)).toBe(0);
    expect(perPublisher.every((n) => n > 0)).toBe(true); // os dois de fato trabalharam em paralelo
    const received = (await drain(client, gateway.urls.events)).map((m) => JSON.parse(m.body).eventId);
    expect(new Set(received.filter((id: string) => eventIds.includes(id))).size).toBe(eventIds.length);
  });

  test('crash depois do SendMessage e antes do commit: outro publisher republica; nada se perde, duplicata é segura', async () => {
    const { eventIds } = await produceEvents(1);
    const sent = new Map<string, number>();
    const crashing = new OutboxPublisher(app.orm, countingGateway(gateway, sent), new AppMetrics(), logger, {
      batchSize: 50,
      hooks: {
        afterSend: () => {
          throw new Error('simulated crash before commit');
        },
      },
    });
    await expect(crashing.publishOnce()).rejects.toThrow('simulated crash');
    expect(await unpublished(app.orm, eventIds)).toBe(eventIds.length); // rollback: continuam pendentes

    const survivor = new OutboxPublisher(app.orm, countingGateway(gateway, sent), new AppMetrics(), logger, { batchSize: 50 });
    await publishUntilDone(survivor, app.orm, eventIds);
    for (const id of eventIds) expect(sent.get(id)).toBe(2); // at-least-once: enviado de novo

    // A FIFO deduplica pelo eventId (MessageDeduplicationId); o consumidor final também pode, pelo eventId.
    const received = (await drain(client, gateway.urls.events)).map((m) => JSON.parse(m.body).eventId);
    for (const id of eventIds) expect(received.filter((r: string) => r === id)).toHaveLength(1);
  });

  test('SQS indisponível: evento NÃO é descartado; reagendado com backoff e lag visível; publica quando volta', async () => {
    const { eventIds } = await produceEvents(1);
    const metrics = new AppMetrics();
    const publisher = new OutboxPublisher(app.orm, gateway, metrics, logger, { batchSize: 100 });

    await client.send(new DeleteQueueCommand({ QueueUrl: gateway.urls.events })); // falha real do destino
    for (let i = 0; i < 20 && (await failedOnce(eventIds)) < eventIds.length; i++) await publisher.publishOnce();
    expect(await failedOnce(eventIds)).toBe(eventIds.length);
    expect(await unpublished(app.orm, eventIds)).toBe(eventIds.length);
    const { pending, lagSeconds } = await publisher.refreshLag();
    expect(pending).toBeGreaterThan(0);
    expect(lagSeconds).toBeGreaterThan(0);
    expect((await metrics.outboxPublishFailures.get()).values[0]!.value).toBeGreaterThanOrEqual(eventIds.length);

    await client.send(new CreateQueueCommand({ QueueName: eventsQueueName, Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' } }));
    await publishUntilDone(publisher, app.orm, eventIds, 30_000); // backoff de ~1s e publica
  });
});

async function failedOnce(ids: string[]): Promise<number> {
  const [row] = await app.orm.em
    .fork()
    .execute<{ n: string }[]>(
      `SELECT count(*)::text AS n FROM outbox_messages
        WHERE id = ANY(?::uuid[]) AND attempts >= 1 AND last_error IS NOT NULL AND next_attempt_at > occurred_at AND published_at IS NULL`,
      [`{${ids.join(',')}}`],
    );
  return Number(row?.n ?? 0);
}
