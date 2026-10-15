import type { InboxMessage } from '../../domain/messaging/inbox-message';
import type { OutboxMessage } from '../../domain/messaging/outbox-message';
import type { WagerTransaction } from '../../domain/wagering/wager-transaction';
import type { Wallet } from '../../domain/wallet/wallet';
import type { WalletLedgerEntry } from '../../domain/wallet/wallet-ledger-entry';

export interface Clock {
  now(): Date;
}

export interface IdGenerator {
  /** UUIDv7: ordenável por tempo, bom para índices B-tree e para o cursor. */
  next(): string;
}

export interface WalletRepository {
  /** Lança WalletAlreadyExistsError em (player_id, currency) duplicado. */
  insert(wallet: Wallet): Promise<void>;
  findById(id: string): Promise<Wallet | undefined>;
  /** SELECT ... FOR NO KEY UPDATE (decisão A): serializa escritores da MESMA wallet até o commit. */
  lockById(id: string): Promise<Wallet | undefined>;
  /** Persiste saldo/version/updatedAt após debit/credit. */
  saveBalance(wallet: Wallet): Promise<void>;
}

export interface WagerTransactionRepository {
  /** INSERT ... ON CONFLICT DO NOTHING. false = já existe linha com a mesma key ou (provider, externalId). */
  insertIfAbsent(tx: WagerTransaction): Promise<boolean>;
  findById(id: string): Promise<WagerTransaction | undefined>;
  findByIdempotencyKey(key: string): Promise<WagerTransaction | undefined>;
  findByExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction | undefined>;
  /** Persiste os campos mutáveis (status, referência, failureCode, snapshot, tentativas). */
  save(tx: WagerTransaction): Promise<void>;
  /** Existe reversão PROCESSED (qualquer kind) para a referência — decisão B. */
  hasProcessedReversal(referenceTransactionId: string): Promise<boolean>;
}

export interface LedgerPage {
  entries: WalletLedgerEntry[];
  /** Menor wallet_version da página; próxima página = versões abaixo dela. */
  nextBeforeVersion: number | undefined;
}

export interface LedgerRepository {
  append(entry: WalletLedgerEntry): Promise<void>;
  /** Ordem decrescente de wallet_version. */
  page(walletId: string, options: { beforeVersion?: number | undefined; limit: number }): Promise<LedgerPage>;
}

export interface OutboxRepository {
  enqueue(messages: OutboxMessage[]): Promise<void>;
}

export interface InboxRepository {
  /** INSERT ... ON CONFLICT DO NOTHING; devolve a linha existente quando já havia. */
  insertIfAbsent(message: InboxMessage): Promise<InboxMessage | undefined>;
  save(message: InboxMessage): Promise<void>;
}

/** Repositórios ligados à MESMA transação SQL. */
export interface TransactionScope {
  wallets: WalletRepository;
  transactions: WagerTransactionRepository;
  ledger: LedgerRepository;
  outbox: OutboxRepository;
  inbox: InboxRepository;
}

export interface UnitOfWork {
  /**
   * Executa `work` numa transação (READ COMMITTED). Em erro transitório de concorrência
   * (deadlock, serialization, lock timeout, violação de wt_single_reversal_uq) a transação
   * inteira é refeita com backoff, até um limite; esgotado, lança TransientInfrastructureError.
   * `work` precisa ser reexecutável: crie objetos de domínio dentro dele.
   */
  run<T>(work: (scope: TransactionScope) => Promise<T>): Promise<T>;
}

export const UNIT_OF_WORK = Symbol('UnitOfWork');
export const CLOCK = Symbol('Clock');
export const ID_GENERATOR = Symbol('IdGenerator');
