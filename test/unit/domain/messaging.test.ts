import { describe, expect, test } from 'bun:test';
import { WalletBalanceChanged, WagerTransactionProcessed } from '../../../src/domain/events/wagering-events';
import { ExponentialBackoff } from '../../../src/domain/messaging/backoff-policy';
import { InboxMessage } from '../../../src/domain/messaging/inbox-message';
import { OutboxMessage } from '../../../src/domain/messaging/outbox-message';
import { WagerTransactionKind as K } from '../../../src/domain/wagering/wager-transaction-kind';
import { LedgerDirection } from '../../../src/domain/wallet/ledger-direction';
import { brl, id, newTx, openWallet, T0 } from '../../support/builders';

const ctx = () => ({ eventId: id(), correlationId: 'corr-1', causationId: 'msg-1', occurredAt: T0 });

describe('IntegrationEvent', () => {
  test('WalletBalanceChanged: tipo/versão no tipo e data com MoneyProps (string), nunca Money', () => {
    const wallet = openWallet('100.00');
    const entry = wallet.debit({ money: brl('25.00'), transactionId: id(), ledgerEntryId: id(), at: T0 });
    const json = WalletBalanceChanged.from(wallet, entry, ctx()).toJSON();
    expect(json.eventType).toBe('WalletBalanceChanged');
    expect(json.version).toBe(1);
    expect(json.aggregateId).toBe(wallet.id);
    expect(json.occurredAt).toBe(T0.toISOString());
    expect(json.data).toEqual({
      walletId: wallet.id,
      transactionId: entry.transactionId,
      direction: LedgerDirection.Debit,
      money: { amount: '25.00', currency: 'BRL' },
      balanceBefore: { amount: '100.00', currency: 'BRL' },
      balanceAfter: { amount: '75.00', currency: 'BRL' },
      walletVersion: 2,
    });
    // round-trip JSON estável: nada de instância de classe dentro do payload
    expect(JSON.parse(JSON.stringify(json))).toEqual(json);
  });

  test('WagerTransactionProcessed exige snapshot', () => {
    const wallet = openWallet();
    const tx = newTx(wallet, K.Loss);
    expect(() => WagerTransactionProcessed.from(tx, ctx())).toThrow();
    tx.markProcessed(undefined, T0, { balance: wallet.balance, walletVersion: wallet.version });
    expect(WagerTransactionProcessed.from(tx, ctx()).toJSON().data.balance).toEqual({ amount: '100.00', currency: 'BRL' });
  });
});

describe('OutboxMessage', () => {
  test('enqueue usa eventId como id e grava o envelope; nasce devido e pendente', () => {
    const wallet = openWallet();
    const entry = wallet.credit({ money: brl('1.00'), transactionId: id(), ledgerEntryId: id(), at: T0 });
    const event = WalletBalanceChanged.from(wallet, entry, ctx());
    const msg = OutboxMessage.enqueue(event);
    expect(msg.id).toBe(event.eventId);
    expect(msg.payload).toEqual(JSON.parse(JSON.stringify(event.toJSON())));
    expect(msg.isPending()).toBe(true);
    expect(msg.isDue(T0)).toBe(true);
  });

  test('scheduleRetry incrementa tentativas e empurra nextAttemptAt; markPublished é idempotente', () => {
    const wallet = openWallet();
    const entry = wallet.credit({ money: brl('1.00'), transactionId: id(), ledgerEntryId: id(), at: T0 });
    const msg = OutboxMessage.enqueue(WalletBalanceChanged.from(wallet, entry, ctx()));
    const fixed = new ExponentialBackoff({ baseMs: 1000, factor: 2, maxMs: 60_000, jitterRatio: 0 });
    msg.scheduleRetry(T0, 'boom', fixed);
    expect(msg.attempts).toBe(1);
    expect(msg.isDue(T0)).toBe(false);
    expect(msg.isDue(new Date(T0.getTime() + 1000))).toBe(true);
    msg.markPublished(T0);
    msg.markPublished(new Date(T0.getTime() + 5000));
    expect(msg.publishedAt).toEqual(T0);
    expect(msg.isPending()).toBe(false);
  });
});

describe('ExponentialBackoff', () => {
  const b = new ExponentialBackoff({ baseMs: 500, factor: 2, maxMs: 30_000, jitterRatio: 0.2 });

  test('cresce exponencialmente até o teto (sem jitter: random = 0)', () => {
    expect([1, 2, 3, 4, 7, 8, 20].map((n) => b.delayMs(n, () => 0))).toEqual([500, 1000, 2000, 4000, 30_000, 30_000, 30_000]);
  });

  test('jitter fica em [80%, 100%] do atraso nominal', () => {
    expect(b.delayMs(3, () => 1)).toBe(1600);
    for (let i = 0; i < 100; i++) {
      const d = b.delayMs(3);
      expect(d).toBeGreaterThanOrEqual(1600);
      expect(d).toBeLessThanOrEqual(2000);
    }
  });
});

describe('InboxMessage', () => {
  test('receive → markProcessed (idempotente) e comparação de hash', () => {
    const m = InboxMessage.receive({ messageId: 'msg-1', consumerName: 'wager-consumer', payloadHash: 'a'.repeat(64), receivedAt: T0 });
    expect(m.isProcessed()).toBe(false);
    m.markProcessed(T0, 'tx-1');
    m.markProcessed(new Date(T0.getTime() + 1), 'tx-2');
    expect([m.processedAt, m.transactionId]).toEqual([T0, 'tx-1']);
    expect(m.matchesPayload('b'.repeat(64))).toBe(false);
  });
});
