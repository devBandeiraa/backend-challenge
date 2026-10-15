/**
 * Wallets distintas em paralelo + prova de que o lock é POR WALLET (nunca global).
 */
import { afterAll, beforeAll, expect, test } from 'bun:test';
import {
  type AppContext,
  assertLedgerInvariant,
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

test('20 wallets × 10 apostas simultâneas (200 operações): cada wallet com o saldo exato', async () => {
  const [a] = instances;
  const wallets = await Promise.all(Array.from({ length: 20 }, () => openTestWallet(a!, '100.00')));
  const jobs = wallets.flatMap((w) => Array.from({ length: 10 }, () => wagerCommand(w, { money: { amount: '7.00', currency: 'BRL' } })));

  const results = await inParallel(jobs.length, (i) => instances[i % 3]!.processTransaction.execute(jobs[i]!));
  expect(results.every((r) => r.status === 'fulfilled' && r.value.status === 'PROCESSED')).toBe(true);

  for (const w of wallets) {
    expect(await walletBalance(a!.orm, w.id)).toBe('30.00');
    await assertLedgerInvariant(a!.orm, w.id);
  }
});

test('wallet A travada não bloqueia a wallet B (lock por wallet, não global)', async () => {
  const [a, b] = instances;
  const walletA = await openTestWallet(a!, '100.00');
  const walletB = await openTestWallet(a!, '100.00');

  let release!: () => void;
  const released = new Promise<void>((r) => (release = r));
  let locked!: () => void;
  const isLocked = new Promise<void>((r) => (locked = r));
  const holder = a!.orm.em.fork().transactional(async (em) => {
    await em.execute(`SELECT id FROM wallets WHERE id = ? FOR NO KEY UPDATE`, [walletA.id]);
    locked();
    await released;
  });
  await isLocked;

  const pendingA = b!.processTransaction.execute(wagerCommand(walletA, { money: { amount: '10.00', currency: 'BRL' } }));
  const startedB = performance.now();
  const outB = await b!.processTransaction.execute(wagerCommand(walletB, { money: { amount: '10.00', currency: 'BRL' } }));
  const elapsedB = performance.now() - startedB;

  expect(outB.status).toBe('PROCESSED');
  expect(elapsedB).toBeLessThan(1_000); // B não esperou A
  let aDone = false;
  void pendingA.then(() => (aDone = true));
  await Bun.sleep(200);
  expect(aDone).toBe(false); // A continua esperando o próprio lock

  release();
  await holder;
  expect((await pendingA).status).toBe('PROCESSED');
  await assertLedgerInvariant(a!.orm, walletA.id);
  await assertLedgerInvariant(a!.orm, walletB.id);
});
