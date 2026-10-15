/**
 * Operações concorrentes disputando o saldo da MESMA wallet (seção 8), com 3 contextos/pools.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { WagerTransactionOutcome } from '../../src/application/wagering/process-wager-transaction';
import {
  type AppContext,
  assertLedgerInvariant,
  countRows,
  createAppContext,
  inParallel,
  openTestWallet,
  wagerCommand,
  walletBalance,
} from '../support/app-context';

let instances: AppContext[];

beforeAll(async () => {
  instances = await Promise.all([1, 2, 3].map(() => createAppContext({ poolMax: 20 })));
});
afterAll(async () => {
  await Promise.all(instances.map((i) => i.close()));
});

const values = <T>(results: PromiseSettledResult<T>[]): T[] => {
  const rejected = results.find((r) => r.status === 'rejected') as PromiseRejectedResult | undefined;
  if (rejected) throw rejected.reason;
  return (results as PromiseFulfilledResult<T>[]).map((r) => r.value);
};

describe('cenário obrigatório: saldo 100.00, duas apostas de 80.00 simultâneas', () => {
  // Repetido para que a corrida aconteça de fato em várias intercalações, não por sorte.
  test.each(Array.from({ length: 20 }, (_, i) => i + 1))('rodada %d', async () => {
    const [a] = instances;
    const w = await openTestWallet(a!, '100.00');
    const bets = [0, 1].map(() => wagerCommand(w, { money: { amount: '80.00', currency: 'BRL' } }));

    const outcomes = values(await inParallel(2, (i) => instances[i]!.processTransaction.execute(bets[i]!)));

    expect(outcomes.filter((o) => o.status === 'PROCESSED')).toHaveLength(1);
    const rejected = outcomes.filter((o) => o.status === 'REJECTED');
    expect(rejected).toHaveLength(1);
    expect(rejected[0]!.failureCode).toBe('INSUFFICIENT_FUNDS');
    expect(await walletBalance(a!.orm, w.id)).toBe('20.00');
    expect(await countRows(a!.orm, `wallet_ledger_entries WHERE wallet_id = ? AND direction = 'DEBIT'`, [w.id])).toBe(1);

    // nenhum retry duplica o débito: reenviar as duas (em paralelo) só faz replay
    const retries = values(await inParallel(2, (i) => instances[(i + 1) % 3]!.processTransaction.execute(bets[i]!)));
    expect(retries.every((r) => r.idempotentReplay)).toBe(true);
    expect(retries.map((r) => r.status).sort()).toEqual(outcomes.map((o) => o.status).sort());
    expect(await countRows(a!.orm, `wallet_ledger_entries WHERE wallet_id = ? AND direction = 'DEBIT'`, [w.id])).toBe(1);
    await assertLedgerInvariant(a!.orm, w.id);
  });
});

describe('hot wallet', () => {
  test('200 apostas de 1.00 simultâneas numa wallet de 150.00: exatamente 150 aceitas, saldo nunca negativo', async () => {
    const [a] = instances;
    const w = await openTestWallet(a!, '150.00');
    const outcomes = values(
      await inParallel(200, (i) =>
        instances[i % 3]!.processTransaction.execute(wagerCommand(w, { money: { amount: '1.00', currency: 'BRL' } })),
      ),
    );
    expect(outcomes.filter((o) => o.status === 'PROCESSED')).toHaveLength(150);
    expect(outcomes.filter((o) => o.failureCode === 'INSUFFICIENT_FUNDS')).toHaveLength(50);
    expect(await walletBalance(a!.orm, w.id)).toBe('0.00');
    expect(await countRows(a!.orm, `wallet_ledger_entries WHERE wallet_id = ?`, [w.id])).toBe(151); // OPENING + 150
    // saldo observado de cada aposta aceita é único (cada uma viu uma versão diferente)
    const seen = outcomes.filter((o) => o.status === 'PROCESSED').map((o) => o.balance!.amount);
    expect(new Set(seen).size).toBe(150);
    await assertLedgerInvariant(a!.orm, w.id);
  });

  test('BET e WIN misturados em paralelo: saldo final = inicial − apostas aceitas + prêmios', async () => {
    const [a] = instances;
    const w = await openTestWallet(a!, '50.00');
    const commands = Array.from({ length: 120 }, (_, i) =>
      wagerCommand(w, {
        kind: i % 3 === 0 ? 'WIN' : 'BET',
        money: { amount: i % 3 === 0 ? '7.00' : '3.00', currency: 'BRL' },
      }),
    );
    const outcomes: WagerTransactionOutcome[] = values(
      await inParallel(commands.length, (i) => instances[i % 3]!.processTransaction.execute(commands[i]!)),
    );
    let expectedCents = 5000;
    outcomes.forEach((o, i) => {
      if (o.status !== 'PROCESSED') return;
      expectedCents += commands[i]!.kind === 'WIN' ? 700 : -300;
    });
    expect(await walletBalance(a!.orm, w.id)).toBe((expectedCents / 100).toFixed(2));
    expect(outcomes.every((o) => o.status === 'PROCESSED' || o.failureCode === 'INSUFFICIENT_FUNDS')).toBe(true);
    await assertLedgerInvariant(a!.orm, w.id);
  });

  test('REFUND e ROLLBACK da MESMA BET em paralelo: só uma reversão vence (decisão B)', async () => {
    const [a] = instances;
    for (let round = 0; round < 10; round++) {
      const w = await openTestWallet(a!, '100.00');
      const bet = wagerCommand(w, { money: { amount: '40.00', currency: 'BRL' } });
      await a!.processTransaction.execute(bet);
      const reversals = (['REFUND', 'ROLLBACK', 'REFUND', 'ROLLBACK'] as const).map((kind) =>
        wagerCommand(w, { kind, money: { amount: '40.00', currency: 'BRL' }, referenceExternalTransactionId: bet.externalTransactionId }),
      );
      const outcomes = values(await inParallel(reversals.length, (i) => instances[i % 3]!.processTransaction.execute(reversals[i]!)));
      expect(outcomes.filter((o) => o.status === 'PROCESSED')).toHaveLength(1);
      expect(outcomes.filter((o) => o.failureCode === 'REFERENCE_ALREADY_REVERSED')).toHaveLength(3);
      expect(await walletBalance(a!.orm, w.id)).toBe('100.00');
      await assertLedgerInvariant(a!.orm, w.id);
    }
  });
});
