import { defineEntity, p } from '@mikro-orm/core';

// Registros de persistência, separados do domínio (que não conhece o ORM). O schema real
// vem das migrations; estes mapeamentos só descrevem colunas para leitura/escrita.
// numeric é mapeado como string: o valor nunca passa por number entre o banco e o Money.
export const WalletRecord = defineEntity({
  name: 'WalletRecord',
  tableName: 'wallets',
  properties: {
    id: p.uuid().primary(),
    playerId: p.uuid(),
    currency: p.string().length(3),
    balance: p.decimal('string').precision(19).scale(2),
    version: p.integer(),
    createdAt: p.datetime(),
    updatedAt: p.datetime(),
  },
});

export const WagerTransactionRecord = defineEntity({
  name: 'WagerTransactionRecord',
  tableName: 'wager_transactions',
  properties: {
    id: p.uuid().primary(),
    providerId: p.string(),
    externalTransactionId: p.string(),
    idempotencyKey: p.string(),
    payloadHash: p.string(),
    walletId: p.uuid(),
    playerId: p.uuid(),
    roundId: p.string().nullable(),
    gameId: p.string().nullable(),
    kind: p.string(),
    amount: p.decimal('string').precision(19).scale(2),
    currency: p.string().length(3),
    referenceExternalTransactionId: p.string().nullable(),
    referenceTransactionId: p.uuid().nullable(),
    status: p.string(),
    failureCode: p.string().nullable(),
    resultBalance: p.decimal('string').precision(19).scale(2).nullable(),
    resultBalanceCurrency: p.string().length(3).nullable(),
    resultWalletVersion: p.integer().nullable(),
    attempts: p.integer(),
    nextAttemptAt: p.datetime().nullable(),
    correlationId: p.string(),
    createdAt: p.datetime(),
    updatedAt: p.datetime(),
    processedAt: p.datetime().nullable(),
  },
});

export const LedgerEntryRecord = defineEntity({
  name: 'LedgerEntryRecord',
  tableName: 'wallet_ledger_entries',
  properties: {
    id: p.uuid().primary(),
    walletId: p.uuid(),
    transactionId: p.uuid(),
    walletVersion: p.integer(),
    direction: p.string(),
    amount: p.decimal('string').precision(19).scale(2),
    currency: p.string().length(3),
    balanceBefore: p.decimal('string').precision(19).scale(2),
    balanceAfter: p.decimal('string').precision(19).scale(2),
    createdAt: p.datetime(),
  },
});

export const OutboxRecord = defineEntity({
  name: 'OutboxRecord',
  tableName: 'outbox_messages',
  properties: {
    id: p.uuid().primary(),
    aggregateType: p.string(),
    aggregateId: p.uuid(),
    eventType: p.string(),
    eventVersion: p.integer(),
    payload: p.json<Record<string, unknown>>(),
    correlationId: p.string(),
    occurredAt: p.datetime(),
    attempts: p.integer(),
    nextAttemptAt: p.datetime(),
    publishedAt: p.datetime().nullable(),
    lastError: p.string().nullable(),
  },
});

export const InboxRecord = defineEntity({
  name: 'InboxRecord',
  tableName: 'inbox_messages',
  properties: {
    consumerName: p.string().primary(),
    messageId: p.string().primary(),
    payloadHash: p.string(),
    transactionId: p.uuid().nullable(),
    receivedAt: p.datetime(),
    processedAt: p.datetime().nullable(),
  },
});
