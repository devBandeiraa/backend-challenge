import type { InferEntity } from '@mikro-orm/core';
import { InboxMessage } from '../../domain/messaging/inbox-message';
import { Money } from '../../domain/shared/money';
import type { FailureCode } from '../../domain/wagering/failure-code';
import { WagerTransaction } from '../../domain/wagering/wager-transaction';
import type { WagerTransactionKind, WagerTransactionStatus } from '../../domain/wagering/wager-transaction-kind';
import type { LedgerDirection } from '../../domain/wallet/ledger-direction';
import { Wallet } from '../../domain/wallet/wallet';
import { WalletLedgerEntry } from '../../domain/wallet/wallet-ledger-entry';
import type { InboxRecord, LedgerEntryRecord, WagerTransactionRecord, WalletRecord } from './schemas';

// Persistência → domínio sempre via rehydrate (sem revalidar transições).
// Money.from aceita a string exata do NUMERIC: nenhum valor passa por number.

type WalletRow = InferEntity<typeof WalletRecord>;
type TxRow = InferEntity<typeof WagerTransactionRecord>;
type LedgerRow = InferEntity<typeof LedgerEntryRecord>;
type InboxRow = InferEntity<typeof InboxRecord>;

const money = (amount: string, currency: string) => Money.from({ amount, currency });

export function toWallet(row: WalletRow): Wallet {
  return Wallet.rehydrate({
    id: row.id,
    playerId: row.playerId,
    currency: row.currency,
    balance: money(row.balance, row.currency),
    version: row.version,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  });
}

export function fromWallet(wallet: Wallet): WalletRow {
  return {
    id: wallet.id,
    playerId: wallet.playerId,
    currency: wallet.currency,
    balance: wallet.balance.amount,
    version: wallet.version,
    createdAt: wallet.createdAt,
    updatedAt: wallet.updatedAt,
  };
}

export function toWagerTransaction(row: TxRow): WagerTransaction {
  return WagerTransaction.rehydrate({
    id: row.id,
    providerId: row.providerId,
    externalTransactionId: row.externalTransactionId,
    idempotencyKey: row.idempotencyKey,
    payloadHash: row.payloadHash,
    walletId: row.walletId,
    playerId: row.playerId,
    roundId: row.roundId ?? undefined,
    gameId: row.gameId ?? undefined,
    kind: row.kind as WagerTransactionKind,
    money: money(row.amount, row.currency),
    referenceExternalTransactionId: row.referenceExternalTransactionId ?? undefined,
    correlationId: row.correlationId,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    status: row.status as WagerTransactionStatus,
    referenceTransactionId: row.referenceTransactionId ?? undefined,
    failureCode: (row.failureCode ?? undefined) as FailureCode | undefined,
    processedAt: row.processedAt ?? undefined,
    result:
      row.resultBalance != null && row.resultBalanceCurrency != null && row.resultWalletVersion != null
        ? { balance: money(row.resultBalance, row.resultBalanceCurrency), walletVersion: row.resultWalletVersion }
        : undefined,
    attempts: row.attempts,
    nextAttemptAt: row.nextAttemptAt ?? undefined,
  });
}

/** Campos mutáveis (os únicos que o trigger permite alterar). */
export function mutableTxFields(tx: WagerTransaction) {
  return {
    status: tx.status,
    referenceTransactionId: tx.referenceTransactionId ?? null,
    failureCode: tx.failureCode ?? null,
    resultBalance: tx.result?.balance.amount ?? null,
    resultBalanceCurrency: tx.result?.balance.currency ?? null,
    resultWalletVersion: tx.result?.walletVersion ?? null,
    attempts: tx.attempts,
    nextAttemptAt: tx.nextAttemptAt ?? null,
    processedAt: tx.processedAt ?? null,
    updatedAt: tx.updatedAt,
  };
}

export function fromWagerTransaction(tx: WagerTransaction): TxRow {
  return {
    id: tx.id,
    providerId: tx.providerId,
    externalTransactionId: tx.externalTransactionId,
    idempotencyKey: tx.idempotencyKey,
    payloadHash: tx.payloadHash,
    walletId: tx.walletId,
    playerId: tx.playerId,
    roundId: tx.roundId ?? null,
    gameId: tx.gameId ?? null,
    kind: tx.kind,
    amount: tx.money.amount,
    currency: tx.money.currency,
    referenceExternalTransactionId: tx.referenceExternalTransactionId ?? null,
    correlationId: tx.correlationId,
    createdAt: tx.createdAt,
    ...mutableTxFields(tx),
  };
}

export function toLedgerEntry(row: LedgerRow): WalletLedgerEntry {
  return WalletLedgerEntry.rehydrate({
    id: row.id,
    walletId: row.walletId,
    transactionId: row.transactionId,
    walletVersion: row.walletVersion,
    direction: row.direction as LedgerDirection,
    money: money(row.amount, row.currency),
    balanceBefore: money(row.balanceBefore, row.currency),
    balanceAfter: money(row.balanceAfter, row.currency),
    createdAt: row.createdAt,
  });
}

export function fromLedgerEntry(entry: WalletLedgerEntry): LedgerRow {
  return {
    id: entry.id,
    walletId: entry.walletId,
    transactionId: entry.transactionId,
    walletVersion: entry.walletVersion,
    direction: entry.direction,
    amount: entry.money.amount,
    currency: entry.money.currency,
    balanceBefore: entry.balanceBefore.amount,
    balanceAfter: entry.balanceAfter.amount,
    createdAt: entry.createdAt,
  };
}

export function toInboxMessage(row: InboxRow): InboxMessage {
  return InboxMessage.rehydrate({
    messageId: row.messageId,
    consumerName: row.consumerName,
    payloadHash: row.payloadHash,
    receivedAt: row.receivedAt,
    transactionId: row.transactionId ?? undefined,
    processedAt: row.processedAt ?? undefined,
  });
}
