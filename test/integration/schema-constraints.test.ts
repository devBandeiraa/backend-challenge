/**
 * Garantias do schema testadas SEM a aplicação: SQL direto, como faria um bug, um script
 * manual ou outra aplicação no mesmo banco. Se a regra só existisse no código, estes testes falhariam.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { MikroORM } from '@mikro-orm/postgresql';
import { buildOrmConfig } from '../../src/infrastructure/persistence/mikro-orm.config';
import { migrate } from '../../src/infrastructure/persistence/migrate';
import { ledgerInvariantSinceTestStart } from '../support/invariants';
import { createOwnerOrm, TEST_DATABASE_URL, TEST_OWNER_DATABASE_URL } from '../support/db';

let orm: MikroORM;
// Toda wallet que um teste conseguiu confirmar (o schema recusa o resto) termina consistente com o ledger.
ledgerInvariantSinceTestStart(() => orm);
const uuid = () => crypto.randomUUID();
const HASH = 'a'.repeat(64);

beforeAll(async () => {
  // Owner: aqui se testam as garantias do SCHEMA (triggers/constraints), que valem até para o dono das
  // tabelas. Os privilégios da app, que barram antes, têm teste próprio (db-privileges.test.ts).
  orm = await createOwnerOrm(5);
});
afterAll(async () => {
  await orm.close(true);
});

/** Executa statements numa transação e devolve o SQLSTATE do erro (ou undefined se commitou). */
async function sqlState(statements: [string, unknown[]?][]): Promise<string | undefined> {
  try {
    await orm.em.fork().transactional(async (em) => {
      for (const [sql, params] of statements) await em.execute(sql, params ?? []);
    });
    return undefined;
  } catch (error) {
    return (error as { code?: string }).code;
  }
}

async function run(sql: string, params: unknown[] = []) {
  return orm.em.fork().execute(sql, params);
}

const insertWallet = (id: string, balance = '0', player = uuid(), currency = 'BRL'): [string, unknown[]] => [
  `INSERT INTO wallets (id, player_id, currency, balance) VALUES (?, ?, ?, ?)`,
  [id, player, currency, balance],
];

const insertTx = (o: { id?: string; walletId: string; kind?: string; status?: string; amount?: string; ext?: string; key?: string; provider?: string; refExt?: string | null; refId?: string | null; extra?: string }): [string, unknown[]] => {
  const id = o.id ?? uuid();
  const ext = o.ext ?? `ext-${id}`;
  return [
    `INSERT INTO wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
       round_id, game_id, kind, amount, currency, reference_external_transaction_id, reference_transaction_id, status, correlation_id,
       processed_at, result_balance, result_balance_currency, result_wallet_version)
     VALUES (?, ?, ?, ?, ?, ?, ?, 'r', 'g', ?, ?, 'BRL', ?, ?, ?, 'c', ${
       (o.status ?? 'PENDING') === 'PROCESSED' ? `now(), 0, 'BRL', 1` : 'NULL, NULL, NULL, NULL'
     })`,
    [id, o.provider ?? 'p', ext, o.key ?? `key-${ext}`, HASH, o.walletId, uuid(), o.kind ?? 'BET', o.amount ?? '10', o.refExt ?? null, o.refId ?? null, o.status ?? 'PENDING'],
  ];
};

/** Cria wallet com saldo via caminho consistente (wallet + OPENING + ledger na mesma transação). */
async function walletWithBalance(balance: string) {
  const walletId = uuid();
  const txId = uuid();
  const state = await sqlState([
    insertWallet(walletId, balance),
    insertTx({ id: txId, walletId, kind: 'WIN', status: 'PROCESSED', amount: balance }),
    [
      `INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, wallet_version, direction, amount, currency, balance_before, balance_after)
       VALUES (?, ?, ?, 1, 'CREDIT', ?, 'BRL', 0, ?)`,
      [uuid(), walletId, txId, balance, balance],
    ],
  ]);
  expect(state).toBeUndefined();
  return { walletId, txId };
}

describe('wallets', () => {
  test('CHECK balance >= 0', async () => {
    expect(await sqlState([insertWallet(uuid(), '-0.01')])).toBe('23514');
  });

  test('UNIQUE (player_id, currency)', async () => {
    const player = uuid();
    expect(await sqlState([insertWallet(uuid(), '0', player), insertWallet(uuid(), '0', player)])).toBe('23505');
  });

  test('version precisa começar em 1', async () => {
    expect(await sqlState([[`INSERT INTO wallets (id, player_id, currency, balance, version) VALUES (?, ?, 'BRL', 0, 2)`, [uuid(), uuid()]]])).toBe('WL003');
  });

  test('saldo não muda sem lançamento no ledger (verificação deferred no COMMIT)', async () => {
    const { walletId } = await walletWithBalance('100.00');
    expect(await sqlState([[`UPDATE wallets SET balance = 50, version = 2 WHERE id = ?`, [walletId]]])).toBe('WL004');
  });

  test('saldo inicial > 0 sem lançamento de abertura é rejeitado no COMMIT', async () => {
    expect(await sqlState([insertWallet(uuid(), '100.00')])).toBe('WL004');
  });

  test('version só incrementa (+1) quando o saldo muda', async () => {
    const { walletId } = await walletWithBalance('100.00');
    expect(await sqlState([[`UPDATE wallets SET version = 2 WHERE id = ?`, [walletId]]])).toBe('WL003');
    expect(await sqlState([[`UPDATE wallets SET balance = 90, version = 3 WHERE id = ?`, [walletId]]])).toBe('WL003');
  });

  test('identidade imutável e DELETE proibido', async () => {
    const { walletId } = await walletWithBalance('1.00');
    expect(await sqlState([[`UPDATE wallets SET currency = 'USD' WHERE id = ?`, [walletId]]])).toBe('WL003');
    expect(await sqlState([[`DELETE FROM wallets WHERE id = ?`, [walletId]]])).toBe('WL003');
  });
});

describe('wallet_ledger_entries', () => {
  test('append-only: UPDATE, DELETE e TRUNCATE bloqueados', async () => {
    const { walletId } = await walletWithBalance('10.00');
    expect(await sqlState([[`UPDATE wallet_ledger_entries SET amount = 1 WHERE wallet_id = ?`, [walletId]]])).toBe('WL001');
    expect(await sqlState([[`DELETE FROM wallet_ledger_entries WHERE wallet_id = ?`, [walletId]]])).toBe('WL001');
    expect(await sqlState([[`TRUNCATE wallet_ledger_entries CASCADE`]])).toBe('WL001');
  });

  test('CHECK da aritmética balance_before ± amount = balance_after', async () => {
    const walletId = uuid();
    const txId = uuid();
    expect(
      await sqlState([
        insertWallet(walletId, '0'),
        insertTx({ id: txId, walletId, kind: 'WIN', status: 'PROCESSED' }),
        [`INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, wallet_version, direction, amount, currency, balance_before, balance_after)
          VALUES (?, ?, ?, 2, 'CREDIT', 10, 'BRL', 0, 11)`, [uuid(), walletId, txId]],
      ]),
    ).toBe('23514');
  });

  test('encadeamento: balance_before precisa ser o balance_after anterior', async () => {
    const { walletId } = await walletWithBalance('100.00');
    const txId = uuid();
    expect(
      await sqlState([
        insertTx({ id: txId, walletId, status: 'PENDING' }),
        [`INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, wallet_version, direction, amount, currency, balance_before, balance_after)
          VALUES (?, ?, ?, 2, 'DEBIT', 10, 'BRL', 50, 40)`, [uuid(), walletId, txId]],
      ]),
    ).toBe('WL002');
  });

  test('UNIQUE (transaction_id, wallet_id): no máximo um lançamento por transação', async () => {
    const { walletId, txId } = await walletWithBalance('100.00');
    expect(
      await sqlState([
        [`UPDATE wallets SET balance = 90, version = 2 WHERE id = ?`, [walletId]],
        [`INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, wallet_version, direction, amount, currency, balance_before, balance_after)
          VALUES (?, ?, ?, 2, 'DEBIT', 10, 'BRL', 100, 90)`, [uuid(), walletId, txId]],
      ]),
    ).toBe('23505');
  });

  test('FK composta: lançamento na moeda da wallet', async () => {
    const { walletId } = await walletWithBalance('100.00');
    const txId = uuid();
    expect(
      await sqlState([
        insertTx({ id: txId, walletId }),
        [`INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, wallet_version, direction, amount, currency, balance_before, balance_after)
          VALUES (?, ?, ?, 2, 'DEBIT', 10, 'USD', 100, 90)`, [uuid(), walletId, txId]],
      ]),
    ).toBe('23503');
  });
});

describe('wager_transactions', () => {
  test('UNIQUE (provider_id, idempotency_key) e (provider_id, external_transaction_id)', async () => {
    const walletId = uuid();
    expect(await sqlState([insertWallet(walletId), insertTx({ walletId, ext: 'dup' + walletId }), insertTx({ walletId, ext: 'dup' + walletId })])).toBe('23505');
    const key = `shared-${walletId}`;
    // mesma key no MESMO provedor: conflito
    const w2 = uuid();
    expect(await sqlState([insertWallet(w2), insertTx({ walletId: w2, key, provider: 'p1' }), insertTx({ walletId: w2, key, provider: 'p1' })])).toBe('23505');
    // mesma key em provedores DIFERENTES: permitido (escopo por provedor)
    const w3 = uuid();
    expect(await sqlState([insertWallet(w3), insertTx({ walletId: w3, key, provider: 'p1' }), insertTx({ walletId: w3, key, provider: 'p2' })])).toBeUndefined();
  });

  test('REFUND/ROLLBACK exigem referência; BET não pode ter', async () => {
    const walletId = uuid();
    expect(await sqlState([insertWallet(walletId), insertTx({ walletId, kind: 'REFUND' })])).toBe('23514');
    const betWallet = uuid();
    expect(await sqlState([insertWallet(betWallet), insertTx({ walletId: betWallet, kind: 'BET', refExt: 'x' })])).toBe('23514');
  });

  test('estado terminal é imutável; campos de negócio também', async () => {
    const { txId } = await walletWithBalance('5.00');
    expect(await sqlState([[`UPDATE wager_transactions SET status = 'REJECTED', failure_code = 'INSUFFICIENT_FUNDS' WHERE id = ?`, [txId]]])).toBe('WT001');
    const walletId = uuid();
    const pendingId = uuid();
    await sqlState([insertWallet(walletId), insertTx({ id: pendingId, walletId })]);
    expect(await sqlState([[`UPDATE wager_transactions SET amount = 999 WHERE id = ?`, [pendingId]]])).toBe('WT001');
    expect(await sqlState([[`DELETE FROM wager_transactions WHERE id = ?`, [pendingId]]])).toBe('WT001');
  });

  test('referência resolvida precisa ser da MESMA wallet (FK composta)', async () => {
    const a = await walletWithBalance('10.00');
    const otherWallet = uuid();
    expect(
      await sqlState([insertWallet(otherWallet), insertTx({ walletId: otherWallet, kind: 'REFUND', refExt: 'x', refId: a.txId })]),
    ).toBe('23503');
  });

  test('índice parcial: no máximo UMA reversão PROCESSED por referência, de qualquer kind (decisão B)', async () => {
    const { walletId, txId: betId } = await walletWithBalance('10.00');
    const refund = insertTx({ walletId, kind: 'REFUND', refExt: 'b', refId: betId, status: 'PROCESSED' });
    const rollback = insertTx({ walletId, kind: 'ROLLBACK', refExt: 'b', refId: betId, status: 'PROCESSED' });
    expect(await sqlState([refund, rollback])).toBe('23505');
    // rejeitadas não contam: auditar várias tentativas recusadas é permitido
    const rejected = (kind: string) => [
      `INSERT INTO wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
         round_id, game_id, kind, amount, currency, reference_external_transaction_id, reference_transaction_id, status, failure_code,
         correlation_id, result_balance, result_balance_currency, result_wallet_version)
       VALUES (?, 'p', ?, ?, ?, ?, ?, 'r', 'g', ?, 10, 'BRL', 'b', ?, 'REJECTED', 'REFERENCE_ALREADY_REVERSED', 'c', 0, 'BRL', 1)`,
      [uuid(), `e-${uuid()}`, `k-${uuid()}`, HASH, walletId, uuid(), kind, betId],
    ] as [string, unknown[]];
    expect(await sqlState([rejected('REFUND'), rejected('ROLLBACK')])).toBeUndefined();
  });

  test('failure_code obrigatório em REJECTED e só da taxonomia', async () => {
    const walletId = uuid();
    expect(await sqlState([insertWallet(walletId), insertTx({ walletId, status: 'REJECTED' })])).toBe('23514');
  });
});

describe('inbox_messages', () => {
  test('UNIQUE (consumer_name, message_id)', async () => {
    const msg = uuid();
    const ins: [string, unknown[]] = [`INSERT INTO inbox_messages (consumer_name, message_id, payload_hash) VALUES ('c', ?, ?)`, [msg, HASH]];
    expect(await sqlState([ins, ins])).toBe('23505');
  });
});

describe('migrations', () => {
  test('são reversíveis: up → down até zero → up, num database isolado', async () => {
    const dbName = `wagering_migrations_${Date.now()}`;
    await run(`CREATE DATABASE ${dbName}`);
    const url = TEST_OWNER_DATABASE_URL.replace(/\/[^/]+$/, `/${dbName}`);
    try {
      await migrate('up', url, TEST_DATABASE_URL);
      await migrate('reset', url, TEST_DATABASE_URL); // down até 0 e up de novo
      const isolated = await MikroORM.init(buildOrmConfig({ clientUrl: url, poolMax: 1 }));
      try {
        const tables = await isolated.em.fork().execute<{ n: string }[]>(
          `SELECT count(*)::text AS n FROM information_schema.tables WHERE table_schema = 'public' AND table_name <> 'mikro_orm_migrations'`,
        );
        expect(tables[0]?.n).toBe('5');
        await isolated.migrator.down({ to: 0 });
        const left = await isolated.em.fork().execute<{ n: string }[]>(
          `SELECT count(*)::text AS n FROM information_schema.tables WHERE table_schema = 'public' AND table_name <> 'mikro_orm_migrations'`,
        );
        expect(left[0]?.n).toBe('0');
      } finally {
        await isolated.close(true);
      }
    } finally {
      await run(`DROP DATABASE IF EXISTS ${dbName} WITH (FORCE)`);
    }
  });
});
