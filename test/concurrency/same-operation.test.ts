/**
 * Concorrência REAL (Promise.all após barreira, várias conexões, Postgres real), distribuída entre
 * 3 contextos de aplicação com pools próprios, como 3 instâncias. Invariante do ledger ao final.
 */
import { afterAll, beforeAll, expect, test } from 'bun:test';
import type { WagerTransactionOutcome } from '../../src/application/wagering/process-wager-transaction';
import {
  type AppContext,
  countRows,
  createAppContext,
  inParallel,
  openTestWallet,
  wagerCommand,
  walletBalance,
} from '../support/app-context';
import { ledgerInvariantAfterEach } from '../support/invariants';

let instances: AppContext[];
/** Invariante saldo × ledger no afterEach, para toda wallet registrada com touch() no teste corrente. */
const touch = ledgerInvariantAfterEach(() => instances[0]!.orm);

beforeAll(async () => {
  instances = await Promise.all([1, 2, 3].map(() => createAppContext({ poolMax: 20 })));
});
afterAll(async () => {
  await Promise.all(instances.map((i) => i.close()));
});

const fulfilled = <T>(results: PromiseSettledResult<T>[]) => {
  const rejected = results.filter((r) => r.status === 'rejected') as PromiseRejectedResult[];
  if (rejected.length > 0) throw rejected[0]!.reason;
  return (results as PromiseFulfilledResult<T>[]).map((r) => r.value);
};

test('a MESMA aposta enviada 50x em paralelo por 3 instâncias gera exatamente UM débito', async () => {
  const [a] = instances;
  const w = await openTestWallet(a!, '1000.00');
  const command = wagerCommand(w, { money: { amount: '25.00', currency: 'BRL' } });

  const outcomes = fulfilled(await inParallel(50, (i) => instances[i % 3]!.processTransaction.execute({ ...command })));

  const ids = new Set(outcomes.map((o) => o.transactionId));
  expect(ids.size).toBe(1);
  expect(outcomes.every((o) => o.status === 'PROCESSED' && o.balance?.amount === '975.00')).toBe(true);
  expect(outcomes.filter((o) => !o.idempotentReplay)).toHaveLength(1); // só uma "venceu"
  expect(await countRows(a!.orm, `wager_transactions WHERE idempotency_key = ?`, [command.idempotencyKey])).toBe(1);
  expect(await countRows(a!.orm, `wallet_ledger_entries WHERE wallet_id = ? AND direction = 'DEBIT'`, [w.id])).toBe(1);
  expect(await walletBalance(a!.orm, w.id)).toBe('975.00');
  touch(w.id); // invariante conferida no afterEach
});

test('50x a mesma mensagem com payload DIVERGENTE na mesma key: um vence, os outros recebem 409, nenhum efeito extra', async () => {
  const [a] = instances;
  const w = await openTestWallet(a!, '1000.00');
  const base = wagerCommand(w, { money: { amount: '10.00', currency: 'BRL' } });
  const results = await inParallel(50, (i) =>
    instances[i % 3]!.processTransaction.execute({ ...base, money: { amount: i % 2 === 0 ? '10.00' : '11.00', currency: 'BRL' } }),
  );
  const ok = results.filter((r) => r.status === 'fulfilled') as PromiseFulfilledResult<WagerTransactionOutcome>[];
  const conflicts = results.filter((r) => r.status === 'rejected' && (r.reason as { code?: string }).code === 'IDEMPOTENCY_KEY_PAYLOAD_MISMATCH');
  expect(ok.length + conflicts.length).toBe(50);
  expect(ok.length).toBe(25); // exatamente o grupo com o payload que venceu
  expect(await countRows(a!.orm, `wallet_ledger_entries WHERE wallet_id = ? AND direction = 'DEBIT'`, [w.id])).toBe(1);
  touch(w.id); // invariante conferida no afterEach
});
