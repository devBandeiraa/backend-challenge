/**
 * Taxonomia estável de falhas. O provedor decide pela `action` sem interpretar mensagem:
 *  - retry_later: a mesma requisição pode passar depois (estado do sistema muda);
 *  - fix_payload: só uma NOVA transação (novo externalTransactionId) com dados corrigidos;
 *  - give_up:     a operação é inválida para sempre.
 * Toda rejeição persistida é terminal para aquela idempotency key: reenviar a mesma key
 * devolve o mesmo resultado (replay). "retry_later" significa reenviar como NOVA transação.
 */
export const FailureCode = {
  InsufficientFunds: 'INSUFFICIENT_FUNDS',
  ReversalWouldOverdraw: 'REVERSAL_WOULD_OVERDRAW',
  ReferenceNotFound: 'REFERENCE_NOT_FOUND',
  ReferenceMismatch: 'REFERENCE_MISMATCH',
  ReferenceAlreadyReversed: 'REFERENCE_ALREADY_REVERSED',
  ReferenceNotProcessed: 'REFERENCE_NOT_PROCESSED',
  AmountMismatch: 'AMOUNT_MISMATCH',
  CurrencyMismatch: 'CURRENCY_MISMATCH',
  InvalidReferenceKind: 'INVALID_REFERENCE_KIND',
  PlayerMismatch: 'PLAYER_MISMATCH',
  /** Não persistido (FK de wallet_id, decisão D). */
  WalletNotFound: 'WALLET_NOT_FOUND',
  /** Único código do status FAILED. */
  ProcessingFailed: 'PROCESSING_FAILED',
} as const;

export type FailureCode = (typeof FailureCode)[keyof typeof FailureCode];

export type FailureAction = 'retry_later' | 'fix_payload' | 'give_up';

export const FAILURE_CODE_CATALOG: Record<FailureCode, { action: FailureAction; description: string }> = {
  INSUFFICIENT_FUNDS: { action: 'retry_later', description: 'BET maior que o saldo disponível.' },
  REVERSAL_WOULD_OVERDRAW: {
    action: 'give_up',
    description: 'ROLLBACK que debitaria (de WIN/REFUND) deixaria saldo negativo; exige tratamento operacional.',
  },
  REFERENCE_NOT_FOUND: { action: 'fix_payload', description: 'Referência não chegou dentro da janela de retry.' },
  REFERENCE_MISMATCH: {
    action: 'fix_payload',
    description: 'Referência de outro player, wallet, moeda ou rodada.',
  },
  REFERENCE_ALREADY_REVERSED: { action: 'give_up', description: 'A referência já tem uma reversão efetiva.' },
  REFERENCE_NOT_PROCESSED: { action: 'give_up', description: 'A referência existe mas foi REJECTED/FAILED.' },
  AMOUNT_MISMATCH: { action: 'fix_payload', description: 'Valor da reversão difere do valor da referência.' },
  CURRENCY_MISMATCH: { action: 'fix_payload', description: 'Moeda da operação difere da moeda da wallet.' },
  INVALID_REFERENCE_KIND: {
    action: 'fix_payload',
    description: 'REFUND só referencia BET; ROLLBACK só BET, WIN ou REFUND; WIN/LOSS só BET.',
  },
  PLAYER_MISMATCH: { action: 'fix_payload', description: 'A wallet não pertence ao player informado.' },
  WALLET_NOT_FOUND: { action: 'fix_payload', description: 'walletId inexistente (não persistido).' },
  PROCESSING_FAILED: {
    action: 'retry_later',
    description: 'Erro permanente de infraestrutura após esgotar retries; nada foi aplicado.',
  },
};

export function isFailureCode(value: unknown): value is FailureCode {
  return typeof value === 'string' && value in FAILURE_CODE_CATALOG;
}
