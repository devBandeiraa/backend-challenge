/**
 * Evidência da decisão A (ARCHITECTURE.md): por que a wallet é travada com
 * FOR NO KEY UPDATE e não FOR UPDATE.
 *
 * Com insert-first, o INSERT da transação valida a FK wallet_id e pega FOR KEY SHARE
 * na linha da wallet. FOR UPDATE conflita com KEY SHARE; FOR NO KEY UPDATE não.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import { ledgerInvariantSinceTestStart } from '../support/invariants';
import { createTestOrm } from '../support/db';

let orm: MikroORM;
ledgerInvariantSinceTestStart(() => orm);

beforeAll(async () => {
  orm = await createTestOrm();
});
afterAll(async () => {
  await orm.close(true);
});

function barrier(parties: number) {
  let arrived = 0;
  let release!: () => void;
  const all = new Promise<void>((r) => (release = r));
  return async () => {
    if (++arrived === parties) release();
    await all;
  };
}

async function raceTwoWriters(lockClause: 'FOR UPDATE' | 'FOR NO KEY UPDATE') {
  const walletId = crypto.randomUUID();
  await orm.em.fork().execute(
    `INSERT INTO wallets (id, player_id, currency, balance) VALUES (?, ?, 'BRL', 0)`,
    [walletId, crypto.randomUUID()],
  );
  const bothInserted = barrier(2);

  const writer = (n: number) =>
    orm.em.fork().transactional(async (em) => {
      await em.execute(
        `INSERT INTO wager_transactions
           (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
            round_id, game_id, kind, amount, currency, status, correlation_id)
         VALUES (?, 'p', ?, ?, ?, ?, ?, 'r', 'g', 'BET', 1, 'BRL', 'PENDING', 'c')`,
        [crypto.randomUUID(), `ext-${walletId}-${n}`, `key-${walletId}-${n}`, 'a'.repeat(64), walletId, crypto.randomUUID()],
      );
      await bothInserted(); // as duas já seguram FOR KEY SHARE via FK
      await em.execute(`SELECT id FROM wallets WHERE id = ? ${lockClause}`, [walletId]);
    });

  return Promise.allSettled([writer(1), writer(2)]);
}

describe('wallet row lock vs FK KEY SHARE', () => {
  test('FOR UPDATE após insert-first gera deadlock (40P01)', async () => {
    const results = await raceTwoWriters('FOR UPDATE');
    const rejected = results.filter((r) => r.status === 'rejected');
    expect(rejected).toHaveLength(1);
    const reason = (rejected[0] as PromiseRejectedResult).reason as { code?: string; message: string };
    expect(reason.code ?? reason.message).toMatch(/40P01|deadlock/);
  });

  test('FOR NO KEY UPDATE serializa os dois escritores sem deadlock', async () => {
    const results = await raceTwoWriters('FOR NO KEY UPDATE');
    expect(results.map((r) => r.status)).toEqual(['fulfilled', 'fulfilled']);
  });
});
