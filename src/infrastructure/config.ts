import { hostname } from 'node:os';
import { appDatabaseUrl } from './persistence/database-urls';

/** Papéis que uma instância executa. Padrão: todos, então 3 réplicas = 3 consumers + 3 workers concorrentes. */
export type AppRole = 'http' | 'consumer' | 'outbox' | 'pending-reference';
const ALL_ROLES: AppRole[] = ['http', 'consumer', 'outbox', 'pending-reference'];

export interface AppConfig {
  instanceId: string;
  port: number;
  roles: Set<AppRole>;
  db: { url: string; poolMax: number; lockTimeoutMs: number; statementTimeoutMs: number; uowMaxAttempts: number };
  sqs: {
    endpoint: string;
    inputQueue: string;
    deadLetterQueue: string;
    eventsQueue: string;
    consumerName: string;
    /** Recebimentos até o consumer desistir (FAILED + DLQ). < maxReceiveCount do redrive. */
    maxAttempts: number;
    waitTimeSeconds: number;
    visibilityTimeoutSeconds: number;
    batchSize: number;
    /** Tempo máximo para concluir mensagens em andamento no SIGTERM. */
    shutdownGraceMs: number;
  };
  outbox: { intervalMs: number; batchSize: number };
  pendingReference: { intervalMs: number; batchSize: number; maxAttempts: number; baseDelayMs: number; maxDelayMs: number };
}

const num = (name: string, fallback: number) => {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value)) throw new Error(`env ${name} must be a number, got ${raw}`);
  return value;
};

/** Lido na criação da app (não no import): testes sobem instâncias com parâmetros próprios. */
export function loadConfig(): AppConfig {
  const roles = (process.env.APP_ROLES ?? ALL_ROLES.join(','))
    .split(',')
    .map((r) => r.trim())
    .filter(Boolean) as AppRole[];
  for (const r of roles) if (!ALL_ROLES.includes(r)) throw new Error(`unknown APP_ROLES entry: ${r}`);

  const lockTimeoutMs = num('DB_LOCK_TIMEOUT_MS', 5_000);
  const statementTimeoutMs = num('DB_STATEMENT_TIMEOUT_MS', 15_000);
  // statement_timeout é proteção contra instrução TRAVADA, não mecanismo de contenção: espera de lock
  // precisa terminar por lock_timeout (55P03, refeito pela unit of work). Uma instrução pode esperar
  // mais de um lock (ex.: unicidade e FK), por isso o teto passa de 2×. O lock da wallet (fila de hot
  // wallet) é isento e regido só por lock_timeout: ver lockById.
  if (statementTimeoutMs <= 2 * lockTimeoutMs) {
    throw new Error(
      `DB_STATEMENT_TIMEOUT_MS (${statementTimeoutMs}) must be greater than 2 × DB_LOCK_TIMEOUT_MS (${lockTimeoutMs})`,
    );
  }

  return {
    instanceId: process.env.INSTANCE_ID ?? hostname(),
    port: num('PORT', 3000),
    roles: new Set(roles),
    db: {
      // Usuário da aplicação (sem superuser/ownership); migrations usam MIGRATION_DATABASE_URL.
      url: appDatabaseUrl(),
      poolMax: num('DB_POOL_MAX', 10),
      lockTimeoutMs,
      // Teto de qualquer instrução (query travada, rede meio-aberta): sem ele, o heartbeat do consumer
      // seguraria a mensagem (e o grupo FIFO da wallet) indefinidamente.
      statementTimeoutMs,
      uowMaxAttempts: num('UOW_MAX_ATTEMPTS', 5),
    },
    sqs: {
      endpoint: process.env.SQS_ENDPOINT ?? 'http://localhost:4566',
      inputQueue: process.env.SQS_INPUT_QUEUE ?? 'wager-transactions.fifo',
      deadLetterQueue: process.env.SQS_DLQ ?? 'wager-transactions-dlq.fifo',
      eventsQueue: process.env.SQS_EVENTS_QUEUE ?? 'wager-events.fifo',
      consumerName: process.env.SQS_CONSUMER_NAME ?? 'wager-transactions-consumer',
      maxAttempts: num('SQS_CONSUMER_MAX_ATTEMPTS', 5),
      waitTimeSeconds: num('SQS_WAIT_TIME_SECONDS', 10),
      visibilityTimeoutSeconds: num('SQS_VISIBILITY_TIMEOUT_SECONDS', 90),
      batchSize: num('SQS_BATCH_SIZE', 10),
      shutdownGraceMs: num('SHUTDOWN_GRACE_MS', 10_000),
    },
    // Lote 100: com 10 o publisher não acompanhou ~470 eventos/s (docs/load-test-analysis.md).
    outbox: { intervalMs: num('OUTBOX_INTERVAL_MS', 200), batchSize: num('OUTBOX_BATCH_SIZE', 100) },
    pendingReference: {
      intervalMs: num('PENDING_REFERENCE_INTERVAL_MS', 250),
      batchSize: num('PENDING_REFERENCE_BATCH_SIZE', 20),
      maxAttempts: num('PENDING_REFERENCE_MAX_ATTEMPTS', 10),
      baseDelayMs: num('PENDING_REFERENCE_BASE_DELAY_MS', 500),
      maxDelayMs: num('PENDING_REFERENCE_MAX_DELAY_MS', 30_000),
    },
  };
}

/** Backoff da unit of work entre tentativas (teto do ExponentialBackoff de mikro-orm-unit-of-work). */
const UOW_BACKOFF = { baseMs: 10, factor: 2, maxMs: 200 };

/**
 * Pior caso de processamento de UMA entrega, o maior de dois caminhos:
 *  (a) lock: toda tentativa da unit of work para no lock_timeout (55P03, refeita) e a gravação do
 *      FAILED espera mais um lock_timeout: (tentativas + 1) × lock_timeout + backoff;
 *  (b) instrução lenta: as tentativas anteriores param no lock e uma instrução estoura o
 *      statement_timeout (57014, NÃO refeita na mesma entrega); a gravação do FAILED também é
 *      limitada por ele: (tentativas − 1) × lock_timeout + 2 × statement_timeout + backoff.
 * Cada tentativa é dominada por UMA espera longa; o tempo das demais instruções (ms) é desprezível.
 * O mesmo valor (× fator) é o teto de renovações do heartbeat do consumer.
 */
export function worstCaseMessageProcessingMs(db: Pick<AppConfig['db'], 'uowMaxAttempts' | 'lockTimeoutMs' | 'statementTimeoutMs'>): number {
  let backoff = 0;
  for (let n = 1; n < db.uowMaxAttempts; n++) {
    backoff += Math.min(UOW_BACKOFF.maxMs, UOW_BACKOFF.baseMs * UOW_BACKOFF.factor ** (n - 1));
  }
  const lockPath = (db.uowMaxAttempts + 1) * db.lockTimeoutMs + backoff;
  const statementPath = (db.uowMaxAttempts - 1) * db.lockTimeoutMs + 2 * db.statementTimeoutMs + backoff;
  return Math.max(lockPath, statementPath);
}

/** Teto do heartbeat: depois dele a visibilidade expira e a mensagem volta (inbox e key tornam isso seguro). */
export function maxMessageProcessingMs(db: Pick<AppConfig['db'], 'uowMaxAttempts' | 'lockTimeoutMs' | 'statementTimeoutMs'>): number {
  return Math.ceil(worstCaseMessageProcessingMs(db) * VISIBILITY_SAFETY_FACTOR);
}

export const VISIBILITY_SAFETY_FACTOR = 1.5;

/**
 * Validação de startup: o visibility timeout cobre o pior caso com folga de 1,5×, MESMO que um
 * heartbeat falhe. Configuração inconsistente derruba a instância na subida, não em produção.
 */
export function assertConsumerTiming(config: Pick<AppConfig, 'db' | 'sqs'>): void {
  const requiredMs = Math.ceil(worstCaseMessageProcessingMs(config.db) * VISIBILITY_SAFETY_FACTOR);
  if (config.sqs.visibilityTimeoutSeconds * 1_000 < requiredMs) {
    throw new Error(
      `SQS_VISIBILITY_TIMEOUT_SECONDS=${config.sqs.visibilityTimeoutSeconds} is below ${VISIBILITY_SAFETY_FACTOR}× the worst-case ` +
        `processing time (${worstCaseMessageProcessingMs(config.db)} ms from UOW_MAX_ATTEMPTS=${config.db.uowMaxAttempts}, ` +
        `DB_LOCK_TIMEOUT_MS=${config.db.lockTimeoutMs}, DB_STATEMENT_TIMEOUT_MS=${config.db.statementTimeoutMs}); ` +
        `need at least ${Math.ceil(requiredMs / 1_000)} s`,
    );
  }
}

export const APP_CONFIG = Symbol('AppConfig');
