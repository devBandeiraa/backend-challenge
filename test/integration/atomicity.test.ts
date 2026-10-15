/**
 * Atomicidade (§6.5 / §11): inbox, transação, wallet, ledger e outbox são UMA transação SQL.
 * A unit of work é a real (Postgres real); o scope é envelopado só para registrar as escritas e
 * lançar um erro DEPOIS da última delas, antes do COMMIT. Nada pode sobrar.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { TransactionScope, UnitOfWork } from '../../src/application/ports';
import { ProcessWagerTransactionUseCase } from '../../src/application/wagering/process-wager-transaction';
import { SystemClock, UuidV7Generator } from '../../src/infrastructure/system';
import { type AppContext, countRows, createAppContext, openTestWallet, wagerCommand, walletBalance } from '../support/app-context';
import { ledgerInvariantAfterEach } from '../support/invariants';

let app: AppContext;
const touch = ledgerInvariantAfterEach(() => app.orm);

beforeAll(async () => {
  app = await createAppContext({ poolMax: 4 });
});
afterAll(async () => {
  await app.close();
});

const WRITES = /^(insert|insertIfAbsent|save|saveBalance|append|enqueue)$/;

/** UnitOfWork real que falha depois de TODAS as escritas do trabalho (logo antes do COMMIT). */
function failingAfterWrites(real: UnitOfWork, writes: string[]): UnitOfWork {
  const spy = <T extends object>(name: string, repo: T): T =>
    new Proxy(repo, {
      get(target, prop) {
        const value = Reflect.get(target, prop);
        if (typeof value !== 'function') return value;
        return async (...args: unknown[]) => {
          const result = await value.apply(target, args);
          if (WRITES.test(String(prop))) writes.push(`${name}.${String(prop)}`);
          return result;
        };
      },
    });
  return {
    run: (work) =>
      real.run(async (scope: TransactionScope) => {
        await work({
          wallets: spy('wallets', scope.wallets),
          transactions: spy('transactions', scope.transactions),
          ledger: spy('ledger', scope.ledger),
          inbox: spy('inbox', scope.inbox),
          outbox: spy('outbox', scope.outbox),
        });
        throw new Error('crash after all writes, before COMMIT');
      }),
  };
}

async function persistedState(walletId: string, externalTransactionId: string) {
  return {
    balance: await walletBalance(app.orm, walletId),
    version: (await app.orm.em.fork().execute<{ version: number }[]>(`SELECT version FROM wallets WHERE id = ?`, [walletId]))[0]!.version,
    ledgerEntries: await countRows(app.orm, `wallet_ledger_entries WHERE wallet_id = ?`, [walletId]),
    transactions: await countRows(app.orm, `wager_transactions WHERE external_transaction_id = ?`, [externalTransactionId]),
    outboxForTransaction: await countRows(app.orm, `outbox_messages WHERE payload->'data'->>'externalTransactionId' = ?`, [externalTransactionId]),
    balanceChangedForWallet: await countRows(app.orm, `outbox_messages WHERE event_type = 'WalletBalanceChanged' AND aggregate_id = ?`, [walletId]),
  };
}

describe('falha depois de todas as escritas → nada persiste', () => {
  test('entrada SQS: inbox + transação + wallet + ledger + outbox desfeitos juntos', async () => {
    const w = await openTestWallet(app, '100.00');
    touch(w.id);
    const before = await persistedState(w.id, 'none');
    const writes: string[] = [];
    const useCase = new ProcessWagerTransactionUseCase(failingAfterWrites(app.uow, writes), new SystemClock(), new UuidV7Generator());
    const cmd = wagerCommand(w, { money: { amount: '30.00', currency: 'BRL' } });
    const messageId = `atomic-${crypto.randomUUID()}`;

    await expect(useCase.executeFromMessage(cmd, { consumerName: 'atomicity', messageId, payloadHash: 'a'.repeat(64) })).rejects.toThrow(
      'crash after all writes',
    );

    // todas as escritas aconteceram dentro da transação que falhou
    expect(writes).toEqual([
      'inbox.insertIfAbsent',
      'transactions.insertIfAbsent',
      'wallets.saveBalance',
      'transactions.save',
      'ledger.append',
      'outbox.enqueue',
      'inbox.save',
    ]);
    expect(await countRows(app.orm, `inbox_messages WHERE message_id = ?`, [messageId])).toBe(0);
    expect(await persistedState(w.id, cmd.externalTransactionId)).toEqual({ ...before, transactions: 0, outboxForTransaction: 0 });
    expect(await walletBalance(app.orm, w.id)).toBe('100.00');
  });

  test('entrada HTTP: transação + wallet + ledger + outbox desfeitos juntos; o reenvio depois processa uma vez', async () => {
    const w = await openTestWallet(app, '100.00');
    touch(w.id);
    const before = await persistedState(w.id, 'none');
    const writes: string[] = [];
    const failing = new ProcessWagerTransactionUseCase(failingAfterWrites(app.uow, writes), new SystemClock(), new UuidV7Generator());
    const cmd = wagerCommand(w, { money: { amount: '30.00', currency: 'BRL' } });

    await expect(failing.execute(cmd)).rejects.toThrow('crash after all writes');
    expect(writes).toEqual(['transactions.insertIfAbsent', 'wallets.saveBalance', 'transactions.save', 'ledger.append', 'outbox.enqueue']);
    expect(await persistedState(w.id, cmd.externalTransactionId)).toEqual({ ...before, transactions: 0, outboxForTransaction: 0 });

    // nada sobrou que bloqueie o reenvio com a mesma key: processa normalmente, uma única vez
    const retry = await app.processTransaction.execute(cmd);
    expect(retry).toMatchObject({ status: 'PROCESSED', balance: { amount: '70.00' }, idempotentReplay: false });
    expect(await countRows(app.orm, `wallet_ledger_entries WHERE wallet_id = ? AND direction = 'DEBIT'`, [w.id])).toBe(1);
  });
});
