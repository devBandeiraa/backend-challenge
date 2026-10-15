import type { EntityManager } from '@mikro-orm/postgresql';
import type {
  InboxRepository,
  LedgerPage,
  LedgerRepository,
  OutboxRepository,
  TransactionScope,
  WagerTransactionRepository,
  WalletRepository,
} from '../../application/ports';
import { WalletAlreadyExistsError } from '../../application/errors';
import type { InboxMessage } from '../../domain/messaging/inbox-message';
import type { OutboxMessage } from '../../domain/messaging/outbox-message';
import type { WagerTransaction } from '../../domain/wagering/wager-transaction';
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
}

class MikroOrmWagerTransactionRepository implements WagerTransactionRepository {
  constructor(private readonly em: EntityManager) {}

  async insertIfAbsent(tx: WagerTransaction): Promise<boolean> {
    // Sem alvo no ON CONFLICT: cobre idempotency_key E (provider_id, external_transaction_id).
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

  async findByIdempotencyKey(idempotencyKey: string) {
    const row = await this.em.findOne(WagerTransactionRecord, { idempotencyKey }, FRESH);
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
