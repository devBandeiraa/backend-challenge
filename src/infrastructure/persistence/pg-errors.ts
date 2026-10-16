/**
 * Classificação de erros do Postgres por SQLSTATE/constraint, nunca por mensagem.
 *  - retryable:   concorrência; refazer a transação inteira resolve.
 *  - unavailable: banco inacessível; o chamador deve tentar depois (503 / retry SQS).
 *  - other:       bug, violação de integridade ou erro de aplicação; propaga.
 */
export type PgErrorKind = 'retryable' | 'unavailable' | 'other';

const RETRYABLE_SQLSTATES = new Set([
  '40001', // serialization_failure
  '40P01', // deadlock_detected
  '55P03', // lock_not_available
]);

/**
 * Violações de unicidade que significam "perdi uma corrida", não "dado inválido".
 * wt_single_reversal_uq: sob o lock da wallet a regra já detecta a dupla reversão; se o
 * índice disparar, refazer faz a regra ver a reversão confirmada e rejeitar com
 * REFERENCE_ALREADY_REVERSED.
 */
const RETRYABLE_UNIQUE_CONSTRAINTS = new Set(['wt_single_reversal_uq']);

const UNAVAILABLE_SQLSTATE_PREFIXES = ['08']; // connection_exception
const UNAVAILABLE_SQLSTATES = new Set([
  '57P01', // admin_shutdown
  '57P02', // crash_shutdown
  '57P03', // cannot_connect_now
  '53300', // too_many_connections
  '57014', // query_canceled: statement_timeout (instrução lenta/travada); não é refeita na mesma entrega
]);
const UNAVAILABLE_NODE_CODES = new Set(['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ENOTFOUND', 'EAI_AGAIN']);

interface PgLikeError {
  code?: unknown;
  sqlState?: unknown;
  constraint?: unknown;
  name?: unknown;
  message?: unknown;
}

export function sqlStateOf(error: unknown): string | undefined {
  const e = error as PgLikeError | null;
  if (!e || typeof e !== 'object') return undefined;
  const code = typeof e.sqlState === 'string' ? e.sqlState : e.code;
  return typeof code === 'string' ? code : undefined;
}

export function constraintOf(error: unknown): string | undefined {
  const c = (error as PgLikeError | null)?.constraint;
  return typeof c === 'string' ? c : undefined;
}

/**
 * Erro de conexão SEM SQLSTATE (o servidor sumiu, o socket caiu, o pool não deu vaga): o \`pg\` só manda SQLSTATE
 * em erros do servidor, então um erro do driver sem código é, por construção, de conexão. As mensagens cobrem os
 * casos em que o erro nem passa pelo driver do MikroORM (pg-pool, watchdog de query).
 */
const CONNECTION_ERROR_MESSAGE =
  /not queryable|Connection terminated|Connection ended|timeout exceeded when trying to connect|Query read timeout|without a response: connection destroyed|terminating connection|server closed the connection|socket hang up|read ECONNRESET|connect ECONNREFUSED|Client was closed/i;

/** Erros do driver/ORM (o erro e as causas encadeadas). */
const DRIVER_ERROR_NAMES = new Set(['DriverException', 'ConnectionException', 'DatabaseError']);

function chainOf(error: unknown): PgLikeError[] {
  const out: PgLikeError[] = [];
  let current: unknown = error;
  for (let depth = 0; current && typeof current === 'object' && depth < 5; depth++) {
    out.push(current as PgLikeError);
    const next = (current as { cause?: unknown; previous?: unknown }).cause ?? (current as { previous?: unknown }).previous;
    if (next === current) break;
    current = next;
  }
  return out;
}

export function classifyPgError(error: unknown): PgErrorKind {
  const chain = chainOf(error);
  for (const e of chain) {
    const state = sqlStateOf(e);
    if (state && RETRYABLE_SQLSTATES.has(state)) return 'retryable';
    if (state === '23505' && RETRYABLE_UNIQUE_CONSTRAINTS.has(constraintOf(e) ?? '')) return 'retryable';
    if (state && (UNAVAILABLE_SQLSTATES.has(state) || UNAVAILABLE_SQLSTATE_PREFIXES.some((p) => state.startsWith(p)))) {
      return 'unavailable';
    }
    if (state && UNAVAILABLE_NODE_CODES.has(state)) return 'unavailable';
    // Qualquer outro SQLSTATE é erro do servidor com significado próprio (integridade, sintaxe...): não é conexão.
    if (state && /^[0-9A-Z]{5}$/.test(state)) return 'other';
  }
  for (const e of chain) {
    const name = typeof e.name === 'string' ? e.name : '';
    const message = typeof e.message === 'string' ? e.message : '';
    if (name === 'ConnectionException') return 'unavailable';
    if (CONNECTION_ERROR_MESSAGE.test(message)) return 'unavailable';
    // Erro do driver sem SQLSTATE: falha de transporte (o servidor não chegou a responder com um erro).
    if (DRIVER_ERROR_NAMES.has(name) && sqlStateOf(e) === undefined) return 'unavailable';
  }
  return 'other';
}

/** Falha de infraestrutura transitória (conexão, disponibilidade ou concorrência), nunca motivo para FAILED. */
export function isTransientDbError(error: unknown): boolean {
  return classifyPgError(error) !== 'other';
}

export function isUniqueViolation(error: unknown, constraint: string): boolean {
  return sqlStateOf(error) === '23505' && constraintOf(error) === constraint;
}
