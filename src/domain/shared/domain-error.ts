/** Violação de regra/invariante de domínio. `code` é estável e legível por máquina. */
export abstract class DomainError extends Error {
  abstract readonly code: string;

  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

export class InvalidMoneyError extends DomainError {
  readonly code = 'INVALID_MONEY';
}

export class CurrencyMismatchError extends DomainError {
  readonly code = 'CURRENCY_MISMATCH';
}

/** Saldo resultante negativo: guarda de invariante (a política decide antes e rejeita com failureCode). */
export class NegativeBalanceError extends DomainError {
  readonly code = 'NEGATIVE_BALANCE';
}

/** Dados de criação inválidos para uma entidade (ex.: REFUND sem referência). */
export class InvalidEntityError extends DomainError {
  readonly code = 'INVALID_ENTITY';
}

/** Transição a partir de estado terminal: erro de programação, não caminho de negócio. */
export class InvalidTransactionStateError extends DomainError {
  readonly code = 'INVALID_TRANSACTION_STATE';
}
