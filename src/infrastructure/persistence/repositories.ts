import { LockMode } from '@mikro-orm/core';
import type { EntityManager } from '@mikro-orm/postgresql';
import type {
  InboxRepository,
  LedgerPage,
  LedgerRepository,
  OutboxRepository,
  ReconciliationSnapshot,
  TransactionScope,
  WagerTransactionRepository,
  WalletRepository,
} from '../../application/ports';
import { WalletAlreadyExistsError } from '../../application/errors';
import type { InboxMessage } from '../../domain/messaging/inbox-message';
import type { OutboxMessage } from '../../domain/messaging/outbox-message';
import type { WagerTransaction } from '../../domain/wagering/wager-transaction';
import { Money } from '../../domain/shared/money';
import type { Wallet } from '../../domain/wallet/wallet';
import type { WalletLedgerEntry } from '../../domain/wallet/wallet-ledger-entry';
import {
  fromLedgerEntry,
  fromWagerTransaction,
  fromWallet,
  mutableTxFields,
  toInboxMessage,
  toLedgerEntry,
  toWagerTransaction,
  toWallet,
} from './mappers';
import { isUniqueViolation } from './pg-errors';
import { InboxRecord, LedgerEntryRecord, OutboxRecord, WagerTransactionRecord, WalletRecord } from './schemas';

// Escritas nativas (insert/nativeUpdate), sem flush do Unit of Work: a ordem dos comandos
// importa (FKs, trigger de encadeamento do ledger) e fica explícita no caso de uso.
// Leituras com disableIdentityMap: sempre o estado confirmado/travado no banco, nunca cache.
const FRESH = { disableIdentityMap: true } as const;

class MikroOrmWalletRepository implements WalletRepository {
  constructor(private readonly em: EntityManager) {}

  async insert(wallet: Wallet): Promise<void> {
    try {
      await this.em.insert(WalletRecord, fromWallet(wallet));
    } catch (error) {
      if (isUniqueViolation(error, 'wallets_player_currency_uq')) {
        throw new WalletAlreadyExistsError(`player ${wallet.playerId} already has a ${wallet.currency} wallet`);
      }
      throw error;
    }
  }

  async findById(id: string): Promise<Wallet | undefined> {
    const row = await this.em.findOne(WalletRecord, { id }, FRESH);
    return row ? toWallet(row) : undefined;
  }

  async lockById(id: string): Promise<Wallet | undefined> {
    // FOR NO KEY UPDATE e não LockMode.PESSIMISTIC_WRITE (FOR UPDATE): ver decisão A.
    const rows = await this.em.execute(`SELECT * FROM wallets WHERE id = ? FOR NO KEY UPDATE`, [id]);
    const row = rows[0];
    return row ? toWallet(this.em.map(WalletRecord, row)) : undefined;
  }

  async saveBalance(wallet: Wallet): Promise<void> {
    await this.em.nativeUpdate(
      WalletRecord,
      { id: wallet.id },
      { balance: wallet.balance.amount, version: wallet.version, updatedAt: wallet.updatedAt },
    );
  }

  async reconciliationSnapshot(walletId: string): Promise<ReconciliationSnapshot | undefined> {
    // Uma instrução = um snapshot: saldo e agregados do ledger vistos no mesmo instante.
    // Somas em NUMERIC e devolvidas como texto: nenhum valor passa por number.
    const rows = await this.em.execute<
      (Record<string, unknown> & { credits: string; debits: string; entries: string; last_version: number | null })[]
    >(
      `SELECT w.*,
              COALESCE(SUM(l.amount) FILTER (WHERE l.direction = 'CREDIT'), 0)::numeric(19,2)::text AS credits,
              COALESCE(SUM(l.amount) FILTER (WHERE l.direction = 'DEBIT'), 0)::numeric(19,2)::text  AS debits,
              COUNT(l.id)::text AS entries,
              MAX(l.wallet_version) AS last_version
         FROM wallets w
         LEFT JOIN wallet_ledger_entries l ON l.wallet_id = w.id
        WHERE w.id = ?
        GROUP BY w.id`,
      [walletId],
    );
    const row = rows[0];
    if (!row) return undefined;
    const wallet = toWallet(this.em.map(WalletRecord, row));
    return {
      wallet,
      credits: Money.from({ amount: row.credits, currency: wallet.currency }),
      debits: Money.from({ amount: row.debits, currency: wallet.currency }),
      entries: Number(row.entries),
      lastLedgerVersion: row.last_version ?? undefined,
    };
  }
}

class MikroOrmWagerTransactionRepository implements WagerTransactionRepository {
  constructor(private readonly em: EntityManager) {}

  async insertIfAbsent(tx: WagerTransaction): Promise<boolean> {
    // Sem alvo no ON CONFLICT: cobre (provider_id, idempotency_key) E (provider_id, external_transaction_id).
    const result = await this.em
      .createQueryBuilder(WagerTransactionRecord)
      .insert(fromWagerTransaction(tx))
      .onConflict()
      .ignore()
      .execute('run');
    return result.affectedRows === 1;
  }

  async findById(id: string) {
    const row = await this.em.findOne(WagerTransactionRecord, { id }, FRESH);
    return row ? toWagerTransaction(row) : undefined;
  }

  async findByIdempotencyKey(providerId: string, idempotencyKey: string) {
    const row = await this.em.findOne(WagerTransactionRecord, { providerId, idempotencyKey }, FRESH);
    return row ? toWagerTransaction(row) : undefined;
  }

  async findByExternalId(providerId: string, externalTransactionId: string) {
    const row = await this.em.findOne(WagerTransactionRecord, { providerId, externalTransactionId }, FRESH);
    return row ? toWagerTransaction(row) : undefined;
  }

  async save(tx: WagerTransaction): Promise<void> {
    await this.em.nativeUpdate(WagerTransactionRecord, { id: tx.id }, mutableTxFields(tx));
  }

  async hasProcessedReversal(referenceTransactionId: string): Promise<boolean> {
    const count = await this.em.count(WagerTransactionRecord, {
      referenceTransactionId,
      status: 'PROCESSED',
      kind: { $in: ['REFUND', 'ROLLBACK'] },
    });
    return count > 0;
  }

  async claimDuePendingReference(now: Date) {
    // LockMode.PESSIMISTIC_PARTIAL_WRITE = FOR UPDATE SKIP LOCKED: cada instância pega uma linha
    // diferente; a usada por outra instância é pulada, não esperada.
    const row = await this.em.findOne(
      WagerTransactionRecord,
      { status: 'PENDING_REFERENCE', nextAttemptAt: { $lte: now } },
      { ...FRESH, orderBy: { nextAttemptAt: 'asc' }, lockMode: LockMode.PESSIMISTIC_PARTIAL_WRITE },
    );
    return row ? toWagerTransaction(row) : undefined;
  }
}

class MikroOrmLedgerRepository implements LedgerRepository {
  constructor(private readonly em: EntityManager) {}

  async append(entry: WalletLedgerEntry): Promise<void> {
    await this.em.insert(LedgerEntryRecord, fromLedgerEntry(entry));
  }

  async page(walletId: string, { beforeVersion, limit }: { beforeVersion?: number; limit: number }): Promise<LedgerPage> {
    const rows = await this.em.find(
      LedgerEntryRecord,
      beforeVersion === undefined ? { walletId } : { walletId, walletVersion: { $lt: beforeVersion } },
      { ...FRESH, orderBy: { walletVersion: 'desc' }, limit: limit + 1 },
    );
    const hasMore = rows.length > limit;
    const entries = rows.slice(0, limit).map(toLedgerEntry);
    return { entries, nextBeforeVersion: hasMore ? entries.at(-1)?.walletVersion : undefined };
  }
}

class MikroOrmOutboxRepository implements OutboxRepository {
  constructor(private readonly em: EntityManager) {}

  async enqueue(messages: OutboxMessage[]): Promise<void> {
    if (messages.length === 0) return;
    await this.em.insertMany(
      OutboxRecord,
      messages.map((m) => ({
        id: m.id,
        aggregateType: m.aggregateType,
        aggregateId: m.aggregateId,
        eventType: m.eventType,
        eventVersion: m.eventVersion,
        payload: m.payload as Record<string, unknown>,
        correlationId: m.correlationId,
        occurredAt: m.occurredAt,
        attempts: m.attempts,
        nextAttemptAt: m.nextAttemptAt ?? m.occurredAt,
        publishedAt: m.publishedAt ?? null,
        lastError: m.lastError ?? null,
      })),
    );
  }
}

class MikroOrmInboxRepository implements InboxRepository {
  constructor(private readonly em: EntityManager) {}

  async insertIfAbsent(message: InboxMessage): Promise<InboxMessage | undefined> {
    const result = await this.em
      .createQueryBuilder(InboxRecord)
      .insert({
        consumerName: message.consumerName,
        messageId: message.messageId,
        payloadHash: message.payloadHash,
        transactionId: message.transactionId ?? null,
        receivedAt: message.receivedAt,
        processedAt: message.processedAt ?? null,
      })
      .onConflict()
      .ignore()
      .execute('run');
    if (result.affectedRows === 1) return undefined;
    const existing = await this.em.findOne(
      InboxRecord,
      { consumerName: message.consumerName, messageId: message.messageId },
      FRESH,
    );
    return existing ? toInboxMessage(existing) : undefined;
  }

  async save(message: InboxMessage): Promise<void> {
    await this.em.nativeUpdate(
      InboxRecord,
      { consumerName: message.consumerName, messageId: message.messageId },
      { transactionId: message.transactionId ?? null, processedAt: message.processedAt ?? null },
    );
  }
}

export function createTransactionScope(em: EntityManager): TransactionScope {
  return {
    wallets: new MikroOrmWalletRepository(em),
    transactions: new MikroOrmWagerTransactionRepository(em),
    ledger: new MikroOrmLedgerRepository(em),
    outbox: new MikroOrmOutboxRepository(em),
    inbox: new MikroOrmInboxRepository(em),
  };
}
