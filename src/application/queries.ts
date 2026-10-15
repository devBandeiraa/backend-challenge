import type { MoneyProps } from '../domain/shared/money';
import type { WagerTransaction } from '../domain/wagering/wager-transaction';
import type { WalletLedgerEntry } from '../domain/wallet/wallet-ledger-entry';
import { NotFoundError, ValidationError } from './errors';
import type { UnitOfWork } from './ports';

export interface WalletDetailsView {
  id: string;
  playerId: string;
  balance: MoneyProps;
  version: number;
  createdAt: string;
  updatedAt: string;
}

export interface LedgerEntryView {
  id: string;
  transactionId: string;
  walletVersion: number;
  direction: string;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  createdAt: string;
}

export interface LedgerPageView {
  walletId: string;
  entries: LedgerEntryView[];
  nextCursor: string | null;
}

export interface TransactionView {
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
  referenceTransactionId: string | null;
  status: string;
  failureCode: string | null;
  balance: MoneyProps | null;
  attempts: number;
  nextAttemptAt: string | null;
  createdAt: string;
  processedAt: string | null;
}

export const LEDGER_DEFAULT_LIMIT = 50;
export const LEDGER_MAX_LIMIT = 200;

/**
 * Cursor opaco = base64url({ w: walletId, v: wallet_version }). Estável porque wallet_version
 * é imutável e único por wallet (le_wallet_version_uq): inserções novas não deslocam páginas,
 * diferente de OFFSET. O walletId embutido impede reusar o cursor em outra wallet.
 */
export function encodeLedgerCursor(walletId: string, beforeVersion: number): string {
  return Buffer.from(JSON.stringify({ w: walletId, v: beforeVersion })).toString('base64url');
}

export function decodeLedgerCursor(walletId: string, cursor: string): number {
  try {
    const parsed = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8')) as { w?: unknown; v?: unknown };
    if (parsed.w === walletId && Number.isInteger(parsed.v) && (parsed.v as number) > 0) return parsed.v as number;
  } catch {
    // cai no erro abaixo
  }
  throw new ValidationError('INVALID_CURSOR', 'cursor is invalid or belongs to another wallet');
}

export class WalletQueries {
  constructor(private readonly uow: UnitOfWork) {}

  getWallet(walletId: string): Promise<WalletDetailsView> {
    return this.uow.run(async (scope) => {
      const wallet = await scope.wallets.findById(walletId);
      if (!wallet) throw new NotFoundError(`wallet ${walletId} not found`);
      return {
        id: wallet.id,
        playerId: wallet.playerId,
        balance: wallet.balance.toJSON(),
        version: wallet.version,
        createdAt: wallet.createdAt.toISOString(),
        updatedAt: wallet.updatedAt.toISOString(),
      };
    });
  }

  getLedger(walletId: string, options: { cursor?: string | undefined; limit?: number | undefined }): Promise<LedgerPageView> {
    const limit = options.limit ?? LEDGER_DEFAULT_LIMIT;
    const beforeVersion = options.cursor ? decodeLedgerCursor(walletId, options.cursor) : undefined;
    return this.uow.run(async (scope) => {
      if (!(await scope.wallets.findById(walletId))) throw new NotFoundError(`wallet ${walletId} not found`);
      const page = await scope.ledger.page(walletId, { beforeVersion, limit });
      return {
        walletId,
        entries: page.entries.map(toLedgerEntryView),
        nextCursor: page.nextBeforeVersion !== undefined ? encodeLedgerCursor(walletId, page.nextBeforeVersion) : null,
      };
    });
  }

  getTransaction(transactionId: string): Promise<TransactionView> {
    return this.uow.run(async (scope) => {
      const tx = await scope.transactions.findById(transactionId);
      if (!tx) throw new NotFoundError(`transaction ${transactionId} not found`);
      return toTransactionView(tx);
    });
  }

  getTransactionByExternalId(providerId: string, externalTransactionId: string): Promise<TransactionView> {
    return this.uow.run(async (scope) => {
      const tx = await scope.transactions.findByExternalId(providerId, externalTransactionId);
      if (!tx) throw new NotFoundError(`transaction ${providerId}/${externalTransactionId} not found`);
      return toTransactionView(tx);
    });
  }
}

function toLedgerEntryView(entry: WalletLedgerEntry): LedgerEntryView {
  return {
    id: entry.id,
    transactionId: entry.transactionId,
    walletVersion: entry.walletVersion,
    direction: entry.direction,
    money: entry.money.toJSON(),
    balanceBefore: entry.balanceBefore.toJSON(),
    balanceAfter: entry.balanceAfter.toJSON(),
    createdAt: entry.createdAt.toISOString(),
  };
}

function toTransactionView(tx: WagerTransaction): TransactionView {
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
    referenceTransactionId: tx.referenceTransactionId ?? null,
    status: tx.status,
    failureCode: tx.failureCode ?? null,
    balance: tx.result?.balance.toJSON() ?? null,
    attempts: tx.attempts,
    nextAttemptAt: tx.nextAttemptAt?.toISOString() ?? null,
    createdAt: tx.createdAt.toISOString(),
    processedAt: tx.processedAt?.toISOString() ?? null,
  };
}
