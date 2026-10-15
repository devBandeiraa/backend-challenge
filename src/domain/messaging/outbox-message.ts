import type { IntegrationEvent } from '../events/integration-event';
import { ExponentialBackoff, OUTBOX_BACKOFF } from './backoff-policy';

export interface OutboxMessageState {
  id: string;
  aggregateType: string;
  aggregateId: string;
  eventType: string;
  eventVersion: number;
  payload: Readonly<Record<string, unknown>>;
  correlationId: string;
  occurredAt: Date;
  attempts: number;
  nextAttemptAt: Date | undefined;
  publishedAt: Date | undefined;
  lastError: string | undefined;
}

export class OutboxMessage {
  private constructor(
    public readonly id: string,
    public readonly aggregateType: string,
    public readonly aggregateId: string,
    public readonly eventType: string,
    public readonly eventVersion: number,
    public readonly payload: Readonly<Record<string, unknown>>,
    public readonly correlationId: string,
    public readonly occurredAt: Date,
    private _attempts: number,
    private _nextAttemptAt: Date | undefined,
    private _publishedAt: Date | undefined,
    private _lastError: string | undefined,
  ) {}

  /** id = eventId: o consumidor deduplica por ele, e ele vira o MessageDeduplicationId da FIFO. */
  static enqueue(event: IntegrationEvent<unknown>): OutboxMessage {
    const envelope = event.toJSON();
    return new OutboxMessage(
      event.eventId,
      event.aggregateType,
      event.aggregateId,
      event.eventType,
      event.version,
      // Cópia JSON profunda: o payload gravado é exatamente o que será publicado.
      JSON.parse(JSON.stringify(envelope)) as Record<string, unknown>,
      event.correlationId,
      event.occurredAt,
      0,
      event.occurredAt,
      undefined,
      undefined,
    );
  }

  static rehydrate(state: OutboxMessageState): OutboxMessage {
    return new OutboxMessage(
      state.id,
      state.aggregateType,
      state.aggregateId,
      state.eventType,
      state.eventVersion,
      state.payload,
      state.correlationId,
      state.occurredAt,
      state.attempts,
      state.nextAttemptAt,
      state.publishedAt,
      state.lastError,
    );
  }

  get attempts(): number {
    return this._attempts;
  }
  get nextAttemptAt(): Date | undefined {
    return this._nextAttemptAt;
  }
  get publishedAt(): Date | undefined {
    return this._publishedAt;
  }
  get lastError(): string | undefined {
    return this._lastError;
  }

  isPending(): boolean {
    return this._publishedAt === undefined;
  }

  isDue(now: Date): boolean {
    return this.isPending() && (this._nextAttemptAt === undefined || this._nextAttemptAt <= now);
  }

  markPublished(at: Date): void {
    if (!this.isPending()) return; // idempotente: republicar não muda o histórico
    this._publishedAt = at;
    this._lastError = undefined;
  }

  /** incrementa attempts e calcula o próximo nextAttemptAt (backoff). */
  scheduleRetry(now: Date, error?: string, backoff: ExponentialBackoff = OUTBOX_BACKOFF): void {
    this._attempts += 1;
    this._nextAttemptAt = backoff.nextAttemptAt(now, this._attempts);
    this._lastError = error?.slice(0, 1024);
  }
}
