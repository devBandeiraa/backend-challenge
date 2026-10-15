/**
 * Erros da aplicação, classificados pelo que o CHAMADOR pode fazer. A interface (HTTP/SQS)
 * mapeia `category` para status/ack de forma única, garantindo consistência entre endpoints.
 */
export type ErrorCategory = 'validation' | 'conflict' | 'not_found' | 'business' | 'transient';

export abstract class ApplicationError extends Error {
  abstract readonly category: ErrorCategory;
  abstract readonly code: string;

  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

export class ValidationError extends ApplicationError {
  readonly category = 'validation';
  constructor(
    readonly code: string,
    message: string,
    readonly details?: unknown,
  ) {
    super(message);
  }
}

export class WalletAlreadyExistsError extends ApplicationError {
  readonly category = 'conflict';
  readonly code = 'WALLET_ALREADY_EXISTS';
}

export type IdempotencyConflictCode = 'IDEMPOTENCY_KEY_PAYLOAD_MISMATCH' | 'EXTERNAL_TRANSACTION_ID_CONFLICT';

export class IdempotencyConflictError extends ApplicationError {
  readonly category = 'conflict';
  constructor(
    readonly code: IdempotencyConflictCode,
    message: string,
    readonly existingTransactionId: string,
  ) {
    super(message);
  }
}

/** Recurso consultado inexistente (GET). */
export class NotFoundError extends ApplicationError {
  readonly category = 'not_found';
  readonly code = 'NOT_FOUND';
}

/** Submissão para wallet inexistente: rejeição de negócio NÃO persistida (decisão D). */
export class UnknownWalletError extends ApplicationError {
  readonly category = 'business';
  readonly code = 'WALLET_NOT_FOUND';
  constructor(readonly walletId: string) {
    super(`wallet ${walletId} not found`);
  }
}

/** Banco/fila indisponível ou retries de concorrência esgotados: o chamador deve reenviar. */
export class TransientInfrastructureError extends ApplicationError {
  readonly category = 'transient';
  readonly code = 'TRANSIENT_FAILURE';
  constructor(
    message: string,
    override readonly cause?: unknown,
  ) {
    super(message);
  }
}
