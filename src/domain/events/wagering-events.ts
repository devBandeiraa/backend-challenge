import type { MoneyProps } from '../shared/money';
import type { FailureCode } from '../wagering/failure-code';
import type { WagerTransaction } from '../wagering/wager-transaction';
import type { LedgerDirection } from '../wallet/ledger-direction';
import type { Wallet } from '../wallet/wallet';
import type { WalletLedgerEntry } from '../wallet/wallet-ledger-entry';
import { type EventContext, IntegrationEvent } from './integration-event';

interface TransactionRef {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string | null;
  gameId: string | null;
  kind: string;
  money: MoneyProps;
  referenceExternalTransactionId: string | null;
}

function transactionRef(tx: WagerTransaction): TransactionRef {
  return {
    transactionId: tx.id,
    providerId: tx.providerId,
    externalTransactionId: tx.externalTransactionId,
    walletId: tx.walletId,
    playerId: tx.playerId,
    roundId: tx.roundId ?? null,
    gameId: tx.gameId ?? null,
    kind: tx.kind,
    money: tx.money.toJSON(),
    referenceExternalTransactionId: tx.referenceExternalTransactionId ?? null,
  };
}

function requireResult(tx: WagerTransaction) {
  if (!tx.result) throw new Error(`transaction ${tx.id} has no result snapshot`);
  return tx.result;
}

// ---------------------------------------------------------------------------

export interface WagerTransactionProcessedData extends TransactionRef {
  referenceTransactionId: string | null;
  balance: MoneyProps;
  walletVersion: number;
  processedAt: string;
}

/** Qualquer transação aplicada, inclusive LOSS (sem efeito no saldo) e OPENING. */
export class WagerTransactionProcessed extends IntegrationEvent<WagerTransactionProcessedData> {
  readonly eventType = 'WagerTransactionProcessed';
  readonly version = 1;
  readonly aggregateType = 'WagerTransaction';

  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionProcessed {
    const result = requireResult(tx);
    return new WagerTransactionProcessed({
      ...ctx,
      aggregateId: tx.id,
      data: {
        ...transactionRef(tx),
        referenceTransactionId: tx.referenceTransactionId ?? null,
        balance: result.balance.toJSON(),
        walletVersion: result.walletVersion,
        processedAt: (tx.processedAt ?? ctx.occurredAt).toISOString(),
      },
    });
  }
}

export interface WagerTransactionRejectedData extends TransactionRef {
  failureCode: FailureCode;
  balance: MoneyProps;
  walletVersion: number;
}

export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionRejectedData> {
  readonly eventType = 'WagerTransactionRejected';
  readonly version = 1;
  readonly aggregateType = 'WagerTransaction';

  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionRejected {
    const result = requireResult(tx);
    if (!tx.failureCode) throw new Error(`transaction ${tx.id} has no failureCode`);
    return new WagerTransactionRejected({
      ...ctx,
      aggregateId: tx.id,
      data: {
        ...transactionRef(tx),
        failureCode: tx.failureCode,
        balance: result.balance.toJSON(),
        walletVersion: result.walletVersion,
      },
    });
  }
}

export interface WagerTransactionPendingReferenceData extends TransactionRef {
  attempts: number;
  nextAttemptAt: string | null;
}

export class WagerTransactionPendingReference extends IntegrationEvent<WagerTransactionPendingReferenceData> {
  readonly eventType = 'WagerTransactionPendingReference';
  readonly version = 1;
  readonly aggregateType = 'WagerTransaction';

  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionPendingReference {
    return new WagerTransactionPendingReference({
      ...ctx,
      aggregateId: tx.id,
      data: {
        ...transactionRef(tx),
        attempts: tx.attempts,
        nextAttemptAt: tx.nextAttemptAt?.toISOString() ?? null,
      },
    });
  }
}

export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}

/** Somente quando o saldo muda: construído a partir do lançamento, nunca sem ele. */
export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = 'WalletBalanceChanged';
  readonly version = 1;
  readonly aggregateType = 'Wallet';

  static from(wallet: Wallet, entry: WalletLedgerEntry, ctx: EventContext): WalletBalanceChanged {
    return new WalletBalanceChanged({
      ...ctx,
      aggregateId: wallet.id,
      data: {
        walletId: wallet.id,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        walletVersion: entry.walletVersion,
      },
    });
  }
}
