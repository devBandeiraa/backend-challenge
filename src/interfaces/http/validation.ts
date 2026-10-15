import { z } from 'zod';
import { ValidationError } from '../../application/errors';
import { SUBMITTABLE_KINDS } from '../../domain/wagering/wager-transaction-kind';

// zod valida FORMA e tipos (string, uuid, enum, tamanhos). O conteúdo do dinheiro
// (regex, escala, faixa) é validado só pelo Money do domínio: uma única fonte de verdade.
// .strict(): campo desconhecido é 400. Num WIN, um typo em "referenceExternalTransactionId"
// seria descartado em silêncio; em API financeira, rejeitar é mais seguro que ignorar.

const money = z.object({ amount: z.string(), currency: z.string() }).strict();
export const text = (max: number) => z.string().min(1).max(max);

export const openWalletBody = z
  .object({
    playerId: z.uuid(),
    initialBalance: money,
  })
  .strict();

export const submitTransactionBody = z
  .object({
    providerId: text(64),
    externalTransactionId: text(128),
    playerId: z.uuid(),
    walletId: z.uuid(),
    roundId: text(128),
    gameId: text(128),
    kind: z.enum(SUBMITTABLE_KINDS.map(String) as ['BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK']),
    money,
    referenceExternalTransactionId: text(128).optional(),
  })
  .strict();

export const idempotencyKeyHeader = z
  .string({ error: 'Idempotency-Key header is required' })
  .min(1, 'Idempotency-Key header is required')
  .max(255)
  .regex(/^[\x21-\x7E]+$/, 'Idempotency-Key must be printable ASCII without spaces');

export const uuidParam = z.uuid();

export const ledgerQuery = z
  .object({
    cursor: z.string().min(1).max(512).optional(),
    limit: z.coerce.number().int().min(1).max(200).optional(),
  })
  .strict();

export function parse<S extends z.ZodType>(schema: S, value: unknown, what: string): z.output<S> {
  const result = schema.safeParse(value);
  if (!result.success) {
    throw new ValidationError(
      'INVALID_PAYLOAD',
      `invalid ${what}`,
      result.error.issues.map((i) => ({ path: i.path.join('.'), message: i.message })),
    );
  }
  return result.data;
}
