import { InvalidEntityError, InvalidTransactionStateError } from '../shared/domain-error';
import { Money } from '../shared/money';
import { invertDirection, LedgerDirection } from '../wallet/ledger-direction';
import type { FailureCode } from './failure-code';
import {
  SUBMITTABLE_KINDS,
  TERMINAL_STATUSES,
  WagerTransactionKind,
  WagerTransactionStatus,
} from './wager-transaction-kind';

export interface CreateWagerTransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string | undefined;
  correlationId: string;
  createdAt: Date;
}

/** Saldo da wallet observado quando a transação ficou terminal (replay devolve exatamente isto). */
export interface ResultSnapshot {
  balance: Money;
  walletVersion: number;
}

export interface WagerTransactionState {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string | undefined;
  gameId: string | undefined;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId: string | undefined;
  correlationId: string;
  createdAt: Date;
  updatedAt: Date;
  status: WagerTransactionStatus;
  referenceTransactionId: string | undefined;
  failureCode: FailureCode | undefined;
  processedAt: Date | undefined;
  result: ResultSnapshot | undefined;
  attempts: number;
  nextAttemptAt: Date | undefined;
}

const PAYLOAD_HASH = /^[0-9a-f]{64}$/;
const TEXT_LIMITS = { providerId: 64, externalTransactionId: 128, idempotencyKey: 255, roundId: 128, gameId: 128 };
export const INTERNAL_PROVIDER_ID = 'internal';

/**
 * Transições válidas (também impostas por trigger no banco):
 *
 *   PENDING ──► PROCESSED | REJECTED | FAILED
 *      │
 *      └─────► PENDING_REFERENCE ──► PENDING_REFERENCE (nova tentativa)
 *                                 └► PROCESSED | REJECTED | FAILED
 *
 * PROCESSED, REJECTED e FAILED são terminais: transicionar a partir deles lança
 * InvalidTransactionStateError (erro de programação, não de negócio).
 */
export class WagerTransaction {
  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string | undefined,
    public readonly gameId: string | undefined,
    public readonly kind: WagerTransactionKind,
    public readonly money: Money,
    /** id no provedor, não o id interno */
    public readonly referenceExternalTransactionId: string | undefined,
    public readonly correlationId: string,
    public readonly createdAt: Date,
    private _updatedAt: Date,
    private _status: WagerTransactionStatus,
    private _referenceTransactionId: string | undefined,
    private _failureCode: FailureCode | undefined,
    private _processedAt: Date | undefined,
    private _result: ResultSnapshot | undefined,
    private _attempts: number,
    private _nextAttemptAt: Date | undefined,
  ) {}

  /** Nasce em PENDING. Valida a exigência de referência por kind. OPENING não é aceito aqui. */
  static create(props: CreateWagerTransactionProps): WagerTransaction {
    if (!(SUBMITTABLE_KINDS as readonly WagerTransactionKind[]).includes(props.kind)) {
      throw new InvalidEntityError(`kind ${props.kind} cannot be submitted`);
    }
    for (const field of Object.keys(TEXT_LIMITS) as (keyof typeof TEXT_LIMITS)[]) {
      const value = props[field];
      if (typeof value !== 'string' || value.length === 0 || value.length > TEXT_LIMITS[field]) {
        throw new InvalidEntityError(`${field} must be a non-empty string up to ${TEXT_LIMITS[field]} chars`);
      }
    }
    const needsReference = props.kind === WagerTransactionKind.Refund || props.kind === WagerTransactionKind.Rollback;
    if (needsReference && !props.referenceExternalTransactionId) {
      throw new InvalidEntityError(`${props.kind} requires referenceExternalTransactionId`);
    }
    if (props.kind === WagerTransactionKind.Bet && props.referenceExternalTransactionId !== undefined) {
      throw new InvalidEntityError('BET cannot reference another transaction');
    }
    if (props.referenceExternalTransactionId !== undefined && props.referenceExternalTransactionId.length > 128) {
      throw new InvalidEntityError('referenceExternalTransactionId too long');
    }
    if (props.kind === WagerTransactionKind.Loss ? props.money.isNegative() : !props.money.isPositive()) {
      throw new InvalidEntityError(`${props.kind} amount must be ${props.kind === 'LOSS' ? '>= 0' : '> 0'}`);
    }
    return WagerTransaction.pending(props);
  }

  /** OPENING interno: criado junto com a wallet, já PROCESSED na mesma transação SQL. */
  static createOpening(props: {
    id: string;
    walletId: string;
    playerId: string;
    money: Money;
    payloadHash: string;
    correlationId: string;
    at: Date;
  }): WagerTransaction {
    if (!props.money.isPositive()) throw new InvalidEntityError('OPENING amount must be > 0');
    const tx = WagerTransaction.pending({
      id: props.id,
      providerId: INTERNAL_PROVIDER_ID,
      externalTransactionId: `opening:${props.walletId}`,
      idempotencyKey: `${INTERNAL_PROVIDER_ID}:opening:${props.walletId}`,
      payloadHash: props.payloadHash,
      walletId: props.walletId,
      playerId: props.playerId,
      roundId: undefined,
      gameId: undefined,
      kind: WagerTransactionKind.Opening,
      money: props.money,
      referenceExternalTransactionId: undefined,
      correlationId: props.correlationId,
      createdAt: props.at,
    });
    tx.markProcessed(undefined, props.at, { balance: props.money, walletVersion: 1 });
    return tx;
  }

  /** Reconstrução a partir da persistência: não revalida transições. */
  static rehydrate(state: WagerTransactionState): WagerTransaction {
    return new WagerTransaction(
      state.id,
      state.providerId,
      state.externalTransactionId,
      state.idempotencyKey,
      state.payloadHash,
      state.walletId,
      state.playerId,
      state.roundId,
      state.gameId,
      state.kind,
      state.money,
      state.referenceExternalTransactionId,
      state.correlationId,
      state.createdAt,
      state.updatedAt,
      state.status,
      state.referenceTransactionId,
      state.failureCode,
      state.processedAt,
      state.result,
      state.attempts,
      state.nextAttemptAt,
    );
  }

  private static pending(
    props: Omit<CreateWagerTransactionProps, 'roundId' | 'gameId'> & { roundId: string | undefined; gameId: string | undefined },
  ): WagerTransaction {
    if (!PAYLOAD_HASH.test(props.payloadHash)) throw new InvalidEntityError('payloadHash must be a SHA-256 hex digest');
    return new WagerTransaction(
      props.id,
      props.providerId,
      props.externalTransactionId,
      props.idempotencyKey,
      props.payloadHash,
      props.walletId,
      props.playerId,
      props.roundId,
      props.gameId,
      props.kind,
      props.money,
      props.referenceExternalTransactionId,
      props.correlationId,
      props.createdAt,
      props.createdAt,
      WagerTransactionStatus.Pending,
      undefined,
      undefined,
      undefined,
      undefined,
      0,
      undefined,
    );
  }

  get status(): WagerTransactionStatus {
    return this._status;
  }
  get referenceTransactionId(): string | undefined {
    return this._referenceTransactionId;
  }
  get failureCode(): FailureCode | undefined {
    return this._failureCode;
  }
  get processedAt(): Date | undefined {
    return this._processedAt;
  }
  get result(): ResultSnapshot | undefined {
    return this._result;
  }
  get attempts(): number {
    return this._attempts;
  }
  get nextAttemptAt(): Date | undefined {
    return this._nextAttemptAt;
  }
  get updatedAt(): Date {
    return this._updatedAt;
  }

  // ---- transições

  markProcessed(referenceTransactionId: string | undefined, at: Date, result: ResultSnapshot): void {
    this.assertNotTerminal('markProcessed');
    if (this.requiresReference() && !referenceTransactionId) {
      throw new InvalidTransactionStateError(`${this.kind} cannot be processed without a resolved reference`);
    }
    this._status = WagerTransactionStatus.Processed;
    this._referenceTransactionId = referenceTransactionId;
    this._processedAt = at;
    this._result = result;
    this._nextAttemptAt = undefined;
    this._updatedAt = at;
  }

  /** Cada chamada conta uma tentativa de resolução da referência (a 1ª é o processamento inicial). */
  markPendingReference(nextAttemptAt: Date, at: Date): void {
    this.assertNotTerminal('markPendingReference');
    this._status = WagerTransactionStatus.PendingReference;
    this._attempts += 1;
    this._nextAttemptAt = nextAttemptAt;
    this._updatedAt = at;
  }

  /** referenceTransactionId só quando a referência é da mesma wallet (FK composta no schema). */
  reject(code: FailureCode, at: Date, result: ResultSnapshot, referenceTransactionId?: string): void {
    this.assertNotTerminal('reject');
    this._status = WagerTransactionStatus.Rejected;
    this._failureCode = code;
    this._result = result;
    this._referenceTransactionId = referenceTransactionId;
    this._nextAttemptAt = undefined;
    this._updatedAt = at;
  }

  fail(code: FailureCode, at: Date): void {
    this.assertNotTerminal('fail');
    this._status = WagerTransactionStatus.Failed;
    this._failureCode = code;
    this._nextAttemptAt = undefined;
    this._updatedAt = at;
  }

  // ---- consultas de domínio

  isTerminal(): boolean {
    return TERMINAL_STATUSES.has(this._status);
  }

  affectsBalance(): boolean {
    return this.kind !== WagerTransactionKind.Loss;
  }

  requiresReference(): boolean {
    return this.kind === WagerTransactionKind.Refund || this.kind === WagerTransactionKind.Rollback;
  }

  isReversal(): boolean {
    return this.requiresReference();
  }

  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  /** ROLLBACK inverte a direção da referência; LOSS não tem direção. */
  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.kind) {
      case WagerTransactionKind.Bet:
        return LedgerDirection.Debit;
      case WagerTransactionKind.Opening:
      case WagerTransactionKind.Win:
      case WagerTransactionKind.Refund:
        return LedgerDirection.Credit;
      case WagerTransactionKind.Rollback:
        if (!reference) throw new InvalidTransactionStateError('ROLLBACK direction depends on its reference');
        return invertDirection(reference.ledgerDirectionFor());
      case WagerTransactionKind.Loss:
        throw new InvalidTransactionStateError('LOSS does not move balance');
    }
  }

  private assertNotTerminal(operation: string): void {
    if (this.isTerminal()) {
      throw new InvalidTransactionStateError(`cannot ${operation}: transaction ${this.id} is ${this._status}`);
    }
  }
}
