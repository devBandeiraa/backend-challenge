import { z } from 'zod';
import { INTERNAL_PROVIDER_ID } from '../domain/wagering/wager-transaction';
import { SUBMITTABLE_KINDS } from '../domain/wagering/wager-transaction-kind';

// Contratos de entrada compartilhados por HTTP e SQS (mesmo use case, mesmas regras de forma).
// zod valida FORMA e tipos; o conteúdo do dinheiro (regex, escala, faixa) é validado só pelo
// Money do domínio: uma única fonte de verdade.
// .strict(): campo desconhecido é rejeitado. Num WIN, um typo em "referenceExternalTransactionId"
// seria descartado em silêncio; em API financeira, rejeitar é mais seguro que ignorar.

export const text = (max: number) => z.string().min(1).max(max);
export const moneySchema = z.object({ amount: z.string(), currency: z.string() }).strict();

/** "internal" é o namespace das transações criadas pelo próprio serviço (OPENING): nenhum provedor o usa. */
export const providerIdSchema = text(64).refine((v) => v.toLowerCase() !== INTERNAL_PROVIDER_ID, {
  message: `providerId "${INTERNAL_PROVIDER_ID}" is reserved`,
});

const wagerFields = {
  providerId: providerIdSchema,
  externalTransactionId: text(128),
  playerId: z.uuid(),
  walletId: z.uuid(),
  roundId: text(128),
  gameId: text(128),
  kind: z.enum(SUBMITTABLE_KINDS.map(String) as ['BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK']),
  money: moneySchema,
  referenceExternalTransactionId: text(128).optional(),
};

/** Corpo do POST /wagering/transactions (a key vem no header). */
export const submitTransactionBody = z.object(wagerFields).strict();

export const idempotencyKeySchema = z
  .string({ error: 'Idempotency-Key is required' })
  .min(1, 'Idempotency-Key is required')
  .max(255)
  .regex(/^[\x21-\x7E]+$/, 'Idempotency-Key must be printable ASCII without spaces');

/** `data` da mensagem SQS: validação ESTRITA (a key vem dentro do data). */
export const wagerMessageData = z.object({ ...wagerFields, idempotencyKey: idempotencyKeySchema }).strict();

/**
 * Envelope da mensagem SQS: TOLERANTE. Só `data` é obrigatório; messageId/type/occurredAt são
 * opcionais e campos extras (tracing, versão de schema do produtor...) passam sem quebrar o consumo.
 * Mudanças de envelope do produtor não podem mandar mensagens financeiras para a DLQ.
 */
export const wagerMessageEnvelope = z.looseObject({
  messageId: z.string().min(1).max(128).optional().catch(undefined),
  type: z.string().optional().catch(undefined),
  occurredAt: z.string().optional().catch(undefined),
  data: z.unknown(),
});
