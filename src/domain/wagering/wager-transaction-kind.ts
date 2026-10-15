export enum WagerTransactionKind {
  Opening = 'OPENING', // interno: crédito de abertura da wallet
  Bet = 'BET',
  Win = 'WIN',
  Loss = 'LOSS',
  Refund = 'REFUND',
  Rollback = 'ROLLBACK',
}

/** Kinds aceitos pela API e pela fila. OPENING fica de fora por construção. */
export const SUBMITTABLE_KINDS = [
  WagerTransactionKind.Bet,
  WagerTransactionKind.Win,
  WagerTransactionKind.Loss,
  WagerTransactionKind.Refund,
  WagerTransactionKind.Rollback,
] as const;

export type SubmittableKind = (typeof SUBMITTABLE_KINDS)[number];

export enum WagerTransactionStatus {
  Pending = 'PENDING', // aceita, ainda não aplicada (só existe dentro da transação SQL)
  PendingReference = 'PENDING_REFERENCE', // aguardando a transação referenciada
  Processed = 'PROCESSED', // terminal
  Rejected = 'REJECTED', // terminal: regra de negócio
  Failed = 'FAILED', // terminal: erro permanente de infraestrutura
}

export const TERMINAL_STATUSES: ReadonlySet<WagerTransactionStatus> = new Set([
  WagerTransactionStatus.Processed,
  WagerTransactionStatus.Rejected,
  WagerTransactionStatus.Failed,
]);
