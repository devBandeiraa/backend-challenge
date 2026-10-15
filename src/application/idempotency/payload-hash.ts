import { createHash } from 'node:crypto';
import type { MoneyProps } from '../../domain/shared/money';
import { canonicalJson } from './canonical-json';

/** Campos de NEGÓCIO de uma submissão. Header Idempotency-Key, messageId, occurredAt etc. ficam fora. */
export interface WagerBusinessPayload {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: string;
  /** Validado por Money (escala fixa de 2 casas): uma única grafia por valor. */
  money: MoneyProps;
  referenceExternalTransactionId?: string | undefined;
}

/** Versão do algoritmo entra no conteúdo hasheado: permite evoluir sem colidir com hashes antigos. */
const HASH_VERSION = 'wager-payload/v1';

/**
 * payloadHash = hex(SHA-256(UTF-8(canonicalJson({ v, ...campos de negócio }))))
 * Determinístico entre instâncias e entre HTTP e SQS (mesmo subconjunto de campos).
 */
export function hashWagerPayload(payload: WagerBusinessPayload): string {
  const subset = {
    v: HASH_VERSION,
    providerId: payload.providerId,
    externalTransactionId: payload.externalTransactionId,
    playerId: payload.playerId,
    walletId: payload.walletId,
    roundId: payload.roundId,
    gameId: payload.gameId,
    kind: payload.kind,
    money: { amount: payload.money.amount, currency: payload.money.currency },
    referenceExternalTransactionId: payload.referenceExternalTransactionId,
  };
  return sha256Hex(canonicalJson(subset));
}

export function sha256Hex(input: string): string {
  return createHash('sha256').update(input, 'utf8').digest('hex');
}
