/**
 * Referências fora de ordem: REFUND/ROLLBACK/WIN que chegam antes da transação referenciada,
 * resolvidos pelo worker (SKIP LOCKED) com backoff e limite de tentativas. Postgres real.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { type AppContext, countRows, createAppContext, inParallel, openTestWallet, wagerCommand, walletBalance } from '../support/app-context';
import { ledgerInvariantAfterEach } from '../support/invariants';

let app: AppContext;
const touch = ledgerInvariantAfterEach(() => app.orm);

beforeAll(async () => {
  // Backoff curto (50ms, sem jitter) e 4 tentativas para o teste não depender de minutos.
  app = await createAppContext({ pending: { maxAttempts: 4, baseMs: 50, maxMs: 200 } });
});
afterAll(async () => {
  await app.close();
});

async function wallet(amount = '100.00') {
  const w = await openTestWallet(app, amount);
  touch(w.id);
  return w;
}

async function status(transactionId: string) {
  const [row] = await app.orm.em
    .fork()
    .execute<{ status: string; failure_code: string | null; attempts: number }[]>(
      `SELECT status, failure_code, attempts FROM wager_transactions WHERE id = ?`,
      [transactionId],
    );
  return row!;
}

/** Roda o worker até a transação sair de PENDING_REFERENCE (respeitando next_attempt_at). */
async function resolve(transactionId: string, ctx = app) {
  for (let i = 0; i < 100; i++) {
    if ((await status(transactionId)).status !== 'PENDING_REFERENCE') return status(transactionId);
    await ctx.resolvePending.runOnce(50);
    await Bun.sleep(30);
  }
  throw new Error('still pending');
}

const events = (aggregateId: string, type: string) =>
  countRows(app.orm, `outbox_messages WHERE aggregate_id = ? AND event_type = ?`, [aggregateId, type]);

describe('referência chega depois', () => {
  test('REFUND antes da BET: PENDING_REFERENCE → BET chega → worker processa o REFUND', async () => {
    const w = await wallet('100.00');
    const bet = wagerCommand(w, { money: { amount: '30.00', currency: 'BRL' } });
    const refund = wagerCommand(w, { kind: 'REFUND', money: { amount: '30.00', currency: 'BRL' }, referenceExternalTransactionId: bet.externalTransactionId });

    const pending = await app.processTransaction.execute(refund);
    expect(pending.status).toBe('PENDING_REFERENCE');
    expect(await app.processTransaction.execute(bet)).toMatchObject({ status: 'PROCESSED', balance: { amount: '70.00' } });

    expect(await resolve(pending.transactionId)).toMatchObject({ status: 'PROCESSED' });
    expect(await walletBalance(app.orm, w.id)).toBe('100.00');
    expect(await events(pending.transactionId, 'WagerTransactionPendingReference')).toBe(1); // só na entrada
    expect(await events(pending.transactionId, 'WagerTransactionProcessed')).toBe(1);
    // replay da submissão original agora devolve o resultado final
    expect(await app.processTransaction.execute(refund)).toMatchObject({ status: 'PROCESSED', balance: { amount: '100.00' }, idempotentReplay: true });
  });

  test('ROLLBACK antes da BET: crédito aplicado quando a BET chega', async () => {
    const w = await wallet('50.00');
    const bet = wagerCommand(w, { money: { amount: '20.00', currency: 'BRL' } });
    const rb = await app.processTransaction.execute(
      wagerCommand(w, { kind: 'ROLLBACK', money: { amount: '20.00', currency: 'BRL' }, referenceExternalTransactionId: bet.externalTransactionId }),
    );
    await app.processTransaction.execute(bet);
    expect(await resolve(rb.transactionId)).toMatchObject({ status: 'PROCESSED' });
    expect(await walletBalance(app.orm, w.id)).toBe('50.00');
  });

  test('cadeia fora de ordem: ROLLBACK(REFUND) → REFUND → BET, resolvida em ordem causal', async () => {
    const w = await wallet('100.00');
    const bet = wagerCommand(w, { money: { amount: '40.00', currency: 'BRL' } });
    const refund = wagerCommand(w, { kind: 'REFUND', money: { amount: '40.00', currency: 'BRL' }, referenceExternalTransactionId: bet.externalTransactionId });
    const rbRefund = wagerCommand(w, { kind: 'ROLLBACK', money: { amount: '40.00', currency: 'BRL' }, referenceExternalTransactionId: refund.externalTransactionId });

    const c = await app.processTransaction.execute(rbRefund);
    const b = await app.processTransaction.execute(refund);
    await app.processTransaction.execute(bet);
    expect(await resolve(b.transactionId)).toMatchObject({ status: 'PROCESSED' });
    expect(await resolve(c.transactionId)).toMatchObject({ status: 'PROCESSED' });
    expect(await walletBalance(app.orm, w.id)).toBe('60.00'); // 100 - 40 + 40 - 40
  });
});

describe('referência nunca chega', () => {
  test('WIN com referência ausente: após o limite vira REJECTED/REFERENCE_NOT_FOUND, sem crédito, com evento', async () => {
    const w = await wallet('10.00');
    const win = await app.processTransaction.execute(
      wagerCommand(w, { kind: 'WIN', money: { amount: '500.00', currency: 'BRL' }, referenceExternalTransactionId: 'bet-that-never-comes' }),
    );
    const final = await resolve(win.transactionId);
    expect(final).toMatchObject({ status: 'REJECTED', failure_code: 'REFERENCE_NOT_FOUND', attempts: 3 });
    expect(await walletBalance(app.orm, w.id)).toBe('10.00');
    expect(await events(win.transactionId, 'WagerTransactionRejected')).toBe(1);
    expect(await events(win.transactionId, 'WagerTransactionPendingReference')).toBe(1);
  });

  test('não reprocessa antes de next_attempt_at (backoff respeitado)', async () => {
    const slow = await createAppContext({ poolMax: 3, pending: { maxAttempts: 10, baseMs: 60_000, maxMs: 60_000 } });
    const w = await openTestWallet(slow, '10.00');
    touch(w.id);
    const out = await slow.processTransaction.execute(wagerCommand(w, { kind: 'REFUND', referenceExternalTransactionId: 'later' }));
    await slow.resolvePending.runOnce(500);
    expect(await status(out.transactionId)).toMatchObject({ status: 'PENDING_REFERENCE', attempts: 1 });
    await slow.close();
  });
});

describe('linha venenosa', () => {
  test('erro NÃO transitório ao resolver: vira FAILED/PROCESSING_FAILED (não é reivindicada em loop)', async () => {
    const w = await wallet('100.00');
    const out = await app.processTransaction.execute(wagerCommand(w, { kind: 'REFUND', referenceExternalTransactionId: `poison-${crypto.randomUUID()}` }));
    await Bun.sleep(100);

    // Injeção de falha de PROGRAMAÇÃO (não de infraestrutura) sobre o banco real: a leitura da
    // referência desta transação lança TypeError. Todo o resto é Postgres de verdade.
    const { ResolvePendingReferencesUseCase } = await import('../../src/application/wagering/resolve-pending-references');
    const { SystemClock, UuidV7Generator } = await import('../../src/infrastructure/system');
    const faultyUow = {
      run: <T>(work: (scope: any) => Promise<T>) =>
        app.uow.run((scope) =>
          work({
            ...scope,
            transactions: Object.assign(Object.create(scope.transactions), {
              findByExternalId: async (providerId: string, ext: string) => {
                if (ext.startsWith('poison-')) throw new TypeError('unexpected shape');
                return scope.transactions.findByExternalId(providerId, ext);
              },
            }),
          }),
        ),
    };
    const worker = new ResolvePendingReferencesUseCase(faultyUow, new SystemClock(), new UuidV7Generator());
    for (let i = 0; i < 20 && (await status(out.transactionId)).status === 'PENDING_REFERENCE'; i++) await worker.runOnce(50);

    expect(await status(out.transactionId)).toMatchObject({ status: 'FAILED', failure_code: 'PROCESSING_FAILED' });
    expect(await walletBalance(app.orm, w.id)).toBe('100.00');
  });
});

describe('workers concorrentes', () => {
  test('duas instâncias (pools próprios) resolvendo as mesmas pendências: cada uma processada exatamente uma vez', async () => {
    const w = await wallet('1000.00');
    const pairs = Array.from({ length: 12 }, () => {
      const bet = wagerCommand(w, { money: { amount: '5.00', currency: 'BRL' } });
      const refund = wagerCommand(w, { kind: 'REFUND', money: { amount: '5.00', currency: 'BRL' }, referenceExternalTransactionId: bet.externalTransactionId });
      return { bet, refund };
    });
    const pendingIds: string[] = [];
    for (const p of pairs) pendingIds.push((await app.processTransaction.execute(p.refund)).transactionId);
    for (const p of pairs) await app.processTransaction.execute(p.bet);
    await Bun.sleep(250); // vence o backoff

    const [a, b] = await Promise.all([createAppContext({ poolMax: 4 }), createAppContext({ poolMax: 4 })]);
    const results = await inParallel(2, async (i) => {
      const ctx = i === 0 ? a : b;
      const done: string[] = [];
      for (let k = 0; k < 20; k++) done.push(...(await ctx.resolvePending.runOnce(5)).map((r) => r.transactionId));
      return done;
    });
    await Promise.all([a.close(), b.close()]);

    const handled = results.flatMap((r) => (r.status === 'fulfilled' ? r.value : []));
    const ours = handled.filter((id) => pendingIds.includes(id));
    expect(new Set(ours).size).toBe(ours.length); // nenhuma processada pelas duas
    for (const id of pendingIds) expect((await status(id)).status).toBe('PROCESSED');
    expect(await countRows(app.orm, `wallet_ledger_entries WHERE transaction_id = ANY(?::uuid[])`, [`{${pendingIds.join(',')}}`])).toBe(12);
    expect(await walletBalance(app.orm, w.id)).toBe('1000.00');
  });
});
