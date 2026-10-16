import { z } from 'zod';
import { ValidationError } from '../../application/errors';
import { idempotencyKeySchema, moneySchema, submitTransactionBody, text, uuidSchema } from '../contracts';

export { submitTransactionBody, text };

export const openWalletBody = z
  .object({
    playerId: uuidSchema(),
    initialBalance: moneySchema,
  })
  .strict();

export const idempotencyKeyHeader = idempotencyKeySchema;

export const uuidParam = uuidSchema();

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
