import {
  IdempotencyConflictError,
  InboxPayloadMismatchError,
  TransientInfrastructureError,
  UnknownWalletError,
  ValidationError,
} from '../../application/errors';
import { canonicalJson } from '../../application/idempotency/canonical-json';
import { sha256Hex } from '../../application/idempotency/payload-hash';
import type {
  InboundMessage,
  MessageOutcome,
  ProcessWagerTransactionUseCase,
  SubmitWagerTransactionCommand,
} from '../../application/wagering/process-wager-transaction';
import { ExponentialBackoff } from '../../domain/messaging/backoff-policy';
import type { ReceivedMessage, SqsGateway } from '../../infrastructure/messaging/sqs/sqs-gateway';
import { enrichContext, runWithContext } from '../../infrastructure/observability/context';
import type { Logger } from '../../infrastructure/observability/logger';
import type { AppMetrics } from '../../infrastructure/observability/metrics';
import { wagerMessageData, wagerMessageEnvelope } from '../contracts';

export type Disposition = 'processed' | 'duplicate' | 'business_rejected' | 'retry' | 'dead_lettered' | 'released';

export interface ConsumerOptions {
  consumerName: string;
  /** Recebimentos até desistir: grava FAILED e move para a DLQ (antes do redrive do SQS). */
  maxAttempts: number;
  waitTimeSeconds: number;
  visibilityTimeoutSeconds: number;
  batchSize: number;
  /** Prazo para concluir mensagens em andamento no shutdown. */
  shutdownGraceMs: number;
  /**
   * Teto do heartbeat (pior caso × fator, ver config.ts). Depois dele a visibilidade deixa de ser
   * renovada e a mensagem volta à fila: uma entrega travada não segura o grupo FIFO para sempre.
   * A redelivery concorrente é segura (inbox + idempotency key). Sem valor: sem teto.
   */
  maxProcessingMs?: number;
  retryBackoff?: ExponentialBackoff;
  /** Ponto de injeção de falha para testes de crash (ex.: matar o processo após commit, antes do ack). */
  hooks?: { afterCommit?(message: ReceivedMessage, outcome: MessageOutcome): Promise<void> | void };
}

class PermanentMessageError extends Error {
  constructor(
    readonly reason: string,
    message: string,
  ) {
    super(message);
  }
}

const SAFE_ID = /^[A-Za-z0-9._:-]{1,128}$/;

/**
 * Consumer de wager-transactions.fifo. Mesmo use case do HTTP; diferenças só de transporte:
 *  - ack (DeleteMessage) SOMENTE depois do commit. Crash entre commit e ack → redelivery →
 *    inbox reconhece o messageId → nenhum efeito novo → ack;
 *  - negócio (REJECTED, WALLET_NOT_FOUND) → ack: reentregar não muda o resultado;
 *  - transitório → devolve à fila com backoff (ChangeMessageVisibility) até maxAttempts;
 *  - esgotado/inesperado → FAILED (transação separada) → DLQ; inválido/conflito → DLQ.
 * Concorrência: sequencial DENTRO de um MessageGroupId (= walletId, preserva a ordem FIFO
 * por wallet) e paralelo ENTRE grupos.
 */
export class WagerTransactionConsumer {
  private stopping = false;
  private loop: Promise<void> | undefined;
  private abort = new AbortController();
  private readonly retryBackoff: ExponentialBackoff;

  constructor(
    private readonly gateway: SqsGateway,
    private readonly useCase: ProcessWagerTransactionUseCase,
    private readonly metrics: AppMetrics,
    private readonly logger: Logger,
    private readonly options: ConsumerOptions,
  ) {
    this.retryBackoff =
      options.retryBackoff ?? new ExponentialBackoff({ baseMs: 1_000, factor: 2, maxMs: 60_000, jitterRatio: 0.2 });
  }

  start(): void {
    if (this.loop) return;
    this.stopping = false;
    this.abort = new AbortController();
    this.loop = this.run();
  }

  /**
   * Shutdown (SIGTERM): para de receber, conclui o que está em andamento dentro do prazo e
   * devolve imediatamente à fila (visibility 0) as mensagens recebidas que não começaram.
   * O que passar do prazo volta sozinho quando o visibility timeout expirar: nada se perde.
   */
  async stop(): Promise<void> {
    this.stopping = true;
    this.abort.abort();
    if (!this.loop) return;
    const finished = await Promise.race([
      this.loop.then(() => true),
      Bun.sleep(this.options.shutdownGraceMs).then(() => false),
    ]);
    if (!finished) this.logger.warn({ msg: 'consumer shutdown grace period exceeded; in-flight messages will be redelivered' });
    this.loop = undefined;
  }

  private async run(): Promise<void> {
    while (!this.stopping) {
      let messages: ReceivedMessage[];
      try {
        messages = await this.gateway.receive({
          maxMessages: this.options.batchSize,
          waitTimeSeconds: this.options.waitTimeSeconds,
          visibilityTimeoutSeconds: this.options.visibilityTimeoutSeconds,
          abortSignal: this.abort.signal,
        });
      } catch (error) {
        if (this.stopping) break;
        this.logger.error({ msg: 'sqs receive failed', err: error });
        await Bun.sleep(1_000);
        continue;
      }
      await this.processBatch(messages);
    }
  }

  async processBatch(messages: ReceivedMessage[]): Promise<Disposition[]> {
    const groups = new Map<string, ReceivedMessage[]>();
    for (const m of messages) {
      const key = m.groupId ?? m.sqsMessageId;
      groups.set(key, [...(groups.get(key) ?? []), m]);
    }
    const results = await Promise.all(
      [...groups.values()].map(async (group) => {
        const out: Disposition[] = [];
        for (const message of group) {
          if (this.stopping) {
            out.push(await this.release(message));
            continue;
          }
          out.push(await this.handle(message));
        }
        return out;
      }),
    );
    return results.flat();
  }

  async handle(message: ReceivedMessage): Promise<Disposition> {
    const startedAt = performance.now();
    const disposition = await this.dispatch(message);
    // Latência de TODA entrega, qualquer que seja o desfecho (processada, retry, DLQ...).
    this.metrics.observeLatency('sqs', disposition, startedAt);
    return disposition;
  }

  private async dispatch(message: ReceivedMessage): Promise<Disposition> {
    let parsed: { command: SubmitWagerTransactionCommand; inbound: InboundMessage };
    try {
      parsed = this.parse(message);
    } catch (error) {
      const reason = error instanceof PermanentMessageError ? error.reason : 'invalid_message';
      return this.deadLetter(message, reason, (error as Error).message);
    }
    const { command, inbound } = parsed;
    // Escopo de log da mensagem: toda linha emitida daqui em diante (use case, retries da unit
    // of work, DLQ) carrega messageId/correlationId/walletId/providerId automaticamente.
    return runWithContext(
      { messageId: inbound.messageId, correlationId: command.correlationId, walletId: command.walletId, providerId: command.providerId },
      () => this.process(message, command, inbound),
    );
  }

  private async process(
    message: ReceivedMessage,
    command: SubmitWagerTransactionCommand,
    inbound: InboundMessage,
  ): Promise<Disposition> {
    const log = this.logger.child({ receiveCount: message.receiveCount });
    let outcome: MessageOutcome;
    const stopHeartbeat = this.startHeartbeat(message, log);
    try {
      outcome = await this.useCase.executeFromMessage(command, inbound);
    } catch (error) {
      try {
        return await this.handleFailure(message, command, inbound, error, log);
      } finally {
        stopHeartbeat();
      }
    }
    stopHeartbeat();
    enrichContext({ transactionId: outcome.transactionId });

    // Commit feito. Daqui em diante, falhar significa só redelivery (inbox absorve).
    await this.options.hooks?.afterCommit?.(message, outcome);
    try {
      await this.gateway.ack(message);
    } catch (error) {
      log.warn({ msg: 'ack failed after commit; message will be redelivered and deduplicated by inbox', err: error });
    }
    this.metrics.recordOutcome('sqs', outcome); // redelivery pega pela inbox conta como UMA duplicata (inbox)
    const disposition: Disposition = outcome.inboxDuplicate ? 'duplicate' : 'processed';
    this.metrics.sqsMessages.inc({ outcome: disposition });
    log.info({ msg: 'message handled', disposition, status: outcome.status, failureCode: outcome.failureCode });
    return disposition;
  }

  private async handleFailure(
    message: ReceivedMessage,
    command: SubmitWagerTransactionCommand,
    inbound: InboundMessage,
    error: unknown,
    log: Logger,
  ): Promise<Disposition> {
    if (error instanceof UnknownWalletError) {
      // Rejeição de negócio sem efeito: reentregar dá o mesmo resultado. Ack.
      await this.gateway.ack(message).catch(() => undefined);
      this.metrics.sqsMessages.inc({ outcome: 'business_rejected' });
      log.info({ msg: 'message rejected', failureCode: error.code });
      return 'business_rejected';
    }
    if (error instanceof ValidationError || error instanceof IdempotencyConflictError || error instanceof InboxPayloadMismatchError) {
      // Permanente: o mesmo conteúdo sempre falhará. Para inspeção humana, sem FAILED
      // (não há transação válida/única a registrar).
      return this.deadLetter(message, error.code, error.message);
    }
    // Transitório OU não classificado: retry com backoff até o limite. FAILED é terminal, então
    // nunca nasce de um erro desconhecido na 1ª tentativa: um erro "inesperado" pode ser uma
    // falha de infraestrutura não mapeada (driver, rede) que passa sozinha.
    const transient = error instanceof TransientInfrastructureError;
    if (message.receiveCount < this.options.maxAttempts) {
      const delaySeconds = Math.ceil(this.retryBackoff.delayMs(message.receiveCount) / 1000);
      await this.gateway.changeVisibility(message, delaySeconds).catch(() => undefined);
      this.metrics.sqsRetries.inc();
      this.metrics.sqsMessages.inc({ outcome: 'retry' });
      log.warn({ msg: transient ? 'transient failure; will retry' : 'unexpected failure; will retry', delaySeconds, err: error });
      return 'retry';
    }
    // Limite de recebimentos atingido: FAILED (best-effort, transação separada) e DLQ.
    const reason = transient ? 'retries_exhausted' : 'unexpected_error';
    try {
      const failedId = await this.useCase.recordFailure(command, inbound);
      enrichContext({ transactionId: failedId });
      log.error({ msg: 'giving up on message', reason, err: error });
    } catch (recordError) {
      log.error({ msg: 'could not record FAILED; dead-lettering anyway', reason, err: error, recordError });
    }
    return this.deadLetter(message, reason, (error as Error)?.message);
  }

  /**
   * Heartbeat: enquanto a mensagem é processada, renova a visibilidade a cada 1/3 do timeout.
   * Protege contra processamento mais lento que o previsto (pausa de GC, banco lento): sem ele,
   * a mensagem reapareceria para outro consumer no meio do processamento. A inbox e a idempotency
   * key tornariam isso seguro, mas gastariam um recebimento do limite e um retry à toa.
   * Se o processo morrer, o heartbeat morre junto e a visibilidade expira normalmente.
   * Teto de renovações (maxProcessingMs): uma instrução travada além do pior caso não prende a
   * mensagem indefinidamente; ela volta à fila e outra entrega deduplica pela inbox.
   */
  private startHeartbeat(message: ReceivedMessage, log: Logger): () => void {
    const visibility = this.options.visibilityTimeoutSeconds;
    const intervalMs = Math.max(1_000, Math.floor((visibility * 1_000) / 3));
    const maxRenewals =
      this.options.maxProcessingMs === undefined ? Number.POSITIVE_INFINITY : Math.ceil(this.options.maxProcessingMs / intervalMs);
    let renewals = 0;
    const timer = setInterval(() => {
      if (renewals >= maxRenewals) {
        clearInterval(timer);
        log.warn({ msg: 'visibility heartbeat cap reached; message will become visible again', renewals, maxProcessingMs: this.options.maxProcessingMs });
        return;
      }
      renewals += 1;
      this.gateway.changeVisibility(message, visibility).catch((error) => {
        log.warn({ msg: 'visibility heartbeat failed', err: error });
      });
    }, intervalMs);
    return () => clearInterval(timer);
  }

  private async deadLetter(message: ReceivedMessage, reason: string, detail?: string): Promise<Disposition> {
    try {
      await this.gateway.moveToDeadLetter(message, reason, detail);
      this.metrics.dlq.inc({ reason });
      this.metrics.sqsMessages.inc({ outcome: 'dead_lettered' });
      this.logger.warn({ msg: 'message moved to DLQ', sqsMessageId: message.sqsMessageId, reason });
    } catch (error) {
      // SQS indisponível: a mensagem volta após o visibility timeout e o redrive é a rede de segurança.
      this.logger.error({ msg: 'failed to move message to DLQ', sqsMessageId: message.sqsMessageId, reason, err: error });
    }
    return 'dead_lettered';
  }

  private async release(message: ReceivedMessage): Promise<Disposition> {
    await this.gateway.changeVisibility(message, 0).catch(() => undefined);
    this.metrics.sqsMessages.inc({ outcome: 'released' });
    return 'released';
  }

  /** Envelope tolerante; `data` estrito. */
  private parse(message: ReceivedMessage): { command: SubmitWagerTransactionCommand; inbound: InboundMessage } {
    let json: unknown;
    try {
      json = JSON.parse(message.body);
    } catch {
      throw new PermanentMessageError('invalid_json', 'message body is not JSON');
    }
    const envelope = wagerMessageEnvelope.safeParse(json);
    if (!envelope.success) throw new PermanentMessageError('invalid_envelope', 'message is not an object with data');
    const data = wagerMessageData.safeParse(envelope.data.data);
    if (!data.success) {
      const issues = data.error.issues.map((i) => `${i.path.join('.')}: ${i.message}`).join('; ');
      throw new PermanentMessageError('invalid_data', issues);
    }
    // messageId do produtor identifica a mensagem lógica; sem ele, o id SQS (estável entre redeliveries).
    const messageId = envelope.data.messageId ?? message.sqsMessageId;
    const rawCorrelation = (envelope.data as Record<string, unknown>).correlationId;
    const correlationId = typeof rawCorrelation === 'string' && SAFE_ID.test(rawCorrelation) ? rawCorrelation : messageId;
    const { idempotencyKey, ...fields } = data.data;
    return {
      command: { ...fields, idempotencyKey, correlationId, causationId: messageId },
      inbound: {
        consumerName: this.options.consumerName,
        messageId,
        payloadHash: sha256Hex(canonicalJson(data.data)),
      },
    };
  }
}
