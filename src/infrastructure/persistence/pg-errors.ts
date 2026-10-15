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

export function classifyPgError(error: unknown): PgErrorKind {
  const state = sqlStateOf(error);
  if (state && RETRYABLE_SQLSTATES.has(state)) return 'retryable';
  if (state === '23505' && RETRYABLE_UNIQUE_CONSTRAINTS.has(constraintOf(error) ?? '')) return 'retryable';
  if (state && (UNAVAILABLE_SQLSTATES.has(state) || UNAVAILABLE_SQLSTATE_PREFIXES.some((p) => state.startsWith(p)))) {
    return 'unavailable';
  }
  if (state && UNAVAILABLE_NODE_CODES.has(state)) return 'unavailable';
  const name = (error as PgLikeError | null)?.name;
  if (name === 'ConnectionException') return 'unavailable';
  const message = (error as PgLikeError | null)?.message;
  // pg-pool não tem código para timeout de aquisição de conexão.
  if (typeof message === 'string' && /timeout exceeded when trying to connect|Connection terminated/i.test(message)) {
    return 'unavailable';
  }
  return 'other';
}

export function isUniqueViolation(error: unknown, constraint: string): boolean {
  return sqlStateOf(error) === '23505' && constraintOf(error) === constraint;
}
