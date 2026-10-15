import { collectDefaultMetrics, Counter, Gauge, Histogram, Registry } from 'prom-client';

/**
 * Métricas Prometheus. Um Registry POR INSTÂNCIA (não o global do prom-client): cada réplica
 * expõe as suas e o Prometheus agrega; e várias apps no mesmo processo (testes) não colidem.
 */
export class AppMetrics {
  readonly registry = new Registry();

  /** Transações por status final e origem (http|sqs|worker); replay=true conta duplicata. */
  readonly transactions = new Counter({
    name: 'wagering_transactions_total',
    help: 'Wager transactions by resulting status',
    labelNames: ['status', 'source', 'replay'] as const,
    registers: [this.registry],
  });

  /** Duplicatas detectadas: layer=idempotency (mesma key) | inbox (mesma mensagem SQS). */
  readonly duplicates = new Counter({
    name: 'wagering_duplicates_detected_total',
    help: 'Duplicate submissions detected',
    labelNames: ['layer'] as const,
    registers: [this.registry],
  });

  /** Incrementado a CADA retry de transação SQL por conflito (deadlock, lock timeout, serialization). */
  readonly lockConflicts = new Counter({
    name: 'wagering_db_lock_conflicts_total',
    help: 'Transaction retries caused by lock/concurrency conflicts',
    labelNames: ['sqlstate'] as const,
    registers: [this.registry],
  });

  readonly retriesExhausted = new Counter({
    name: 'wagering_db_retries_exhausted_total',
    help: 'Units of work that gave up after max concurrency retries (answered as 503 / SQS retry)',
    registers: [this.registry],
  });

  readonly sqsMessages = new Counter({
    name: 'wagering_sqs_messages_total',
    help: 'SQS messages by handling outcome',
    labelNames: ['outcome'] as const,
    registers: [this.registry],
  });

  readonly sqsRetries = new Counter({
    name: 'wagering_sqs_retries_total',
    help: 'SQS messages returned to the queue for retry with backoff',
    registers: [this.registry],
  });

  readonly dlq = new Counter({
    name: 'wagering_dlq_messages_total',
    help: 'Messages moved to the dead-letter queue',
    labelNames: ['reason'] as const,
    registers: [this.registry],
  });

  readonly outboxPublished = new Counter({
    name: 'wagering_outbox_published_total',
    help: 'Integration events published from the outbox',
    registers: [this.registry],
  });

  readonly outboxPublishFailures = new Counter({
    name: 'wagering_outbox_publish_failures_total',
    help: 'Outbox publish attempts that failed and were rescheduled',
    registers: [this.registry],
  });

  readonly outboxLag = new Gauge({
    name: 'wagering_outbox_lag_seconds',
    help: 'Age of the oldest unpublished outbox message (0 when empty)',
    registers: [this.registry],
  });

  readonly outboxPending = new Gauge({
    name: 'wagering_outbox_pending_messages',
    help: 'Unpublished outbox messages',
    registers: [this.registry],
  });

  readonly pendingReferenceResolutions = new Counter({
    name: 'wagering_pending_reference_resolutions_total',
    help: 'PENDING_REFERENCE retries by result',
    labelNames: ['result'] as const,
    registers: [this.registry],
  });

  readonly reconciliationMismatches = new Counter({
    name: 'wagering_reconciliation_mismatches_total',
    help: 'Reconciliations where stored balance differs from the ledger',
    registers: [this.registry],
  });

  readonly processingDuration = new Histogram({
    name: 'wagering_processing_duration_seconds',
    help: 'End-to-end processing latency of a wager submission',
    labelNames: ['source'] as const,
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
    registers: [this.registry],
  });

  constructor(options: { defaultMetrics?: boolean } = {}) {
    if (options.defaultMetrics) collectDefaultMetrics({ register: this.registry });
  }

  recordOutcome(source: 'http' | 'sqs' | 'worker', outcome: { status: string; idempotentReplay: boolean }, startedAt: number): void {
    this.transactions.inc({ source, status: outcome.status, replay: String(outcome.idempotentReplay) });
    if (outcome.idempotentReplay) this.duplicates.inc({ layer: 'idempotency' });
    this.processingDuration.observe({ source }, (performance.now() - startedAt) / 1000);
  }
}

export const METRICS = Symbol('AppMetrics');
