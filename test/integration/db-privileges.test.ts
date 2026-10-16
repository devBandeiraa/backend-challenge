/**
 * Menor privilégio (migration 008): com a MESMA conexão que a aplicação usa, o Postgres recusa por
 * privilégio (42501) tudo que apagaria ou reescreveria o ledger, ou que desligaria as garantias do
 * schema. Os triggers continuam valendo para o owner (schema-constraints.test.ts); aqui o ponto é
 * que credenciais da app não bastam para contorná-los.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import { createTestOrm } from '../support/db';
import { ledgerInvariantSinceTestStart } from '../support/invariants';

let orm: MikroORM;
const uuid = () => crypto.randomUUID();
const INSUFFICIENT_PRIVILEGE = '42501';
// Nenhum teste aqui deveria mexer em saldo; se mexer, a invariante acusa.
ledgerInvariantSinceTestStart(() => orm);

beforeAll(async () => {
  orm = await createTestOrm(2);
});
afterAll(async () => {
  await orm.close(true);
});

async function sqlState(sql: string, params: unknown[] = []): Promise<string | undefined> {
  try {
    await orm.em.fork().transactional(async (em) => {
      await em.execute(sql, params);
    });
    return undefined;
  } catch (error) {
    return (error as { code?: string }).code;
  }
}

describe('identidade da aplicação', () => {
  test('não é superuser nem dona das tabelas', async () => {
    const [me] = await orm.em.fork().execute<{ rolsuper: boolean; rolbypassrls: boolean; user: string }[]>(
      `SELECT rolsuper, rolbypassrls, current_user AS user FROM pg_roles WHERE rolname = current_user`,
    );
    expect(me).toMatchObject({ rolsuper: false, rolbypassrls: false });
    const owned = await orm.em
      .fork()
      .execute<{ n: string }[]>(`SELECT count(*)::text AS n FROM pg_tables WHERE schemaname = 'public' AND tableowner = current_user`);
    expect(owned[0]?.n).toBe('0');
  });
});

describe('ledger: append-only também por privilégio', () => {
  test.each([
    ['UPDATE', `UPDATE wallet_ledger_entries SET amount = amount WHERE id = '${uuid()}'`],
    ['DELETE', `DELETE FROM wallet_ledger_entries WHERE id = '${uuid()}'`],
    ['TRUNCATE', `TRUNCATE wallet_ledger_entries`],
    ['DISABLE TRIGGER', `ALTER TABLE wallet_ledger_entries DISABLE TRIGGER ALL`],
  ])('%s no ledger → 42501', async (_label, sql) => {
    expect(await sqlState(sql)).toBe(INSUFFICIENT_PRIVILEGE);
  });

  test('SET session_replication_role (desligaria todos os triggers) → 42501', async () => {
    expect(await sqlState(`SET LOCAL session_replication_role = replica`)).toBe(INSUFFICIENT_PRIVILEGE);
  });
});

describe('demais tabelas: só as colunas mutáveis', () => {
  test.each([
    ['DELETE wallet', `DELETE FROM wallets WHERE id = '${uuid()}'`],
    ['UPDATE identidade da wallet', `UPDATE wallets SET player_id = player_id WHERE id = '${uuid()}'`],
    ['UPDATE valor da transação', `UPDATE wager_transactions SET amount = amount WHERE id = '${uuid()}'`],
    ['DELETE transação', `DELETE FROM wager_transactions WHERE id = '${uuid()}'`],
    ['DELETE outbox', `DELETE FROM outbox_messages WHERE id = '${uuid()}'`],
    ['UPDATE payload da outbox', `UPDATE outbox_messages SET payload = payload WHERE id = '${uuid()}'`],
    ['CREATE TABLE', `CREATE TABLE app_should_not_create (id int)`],
  ])('%s → 42501', async (_label, sql) => {
    expect(await sqlState(sql)).toBe(INSUFFICIENT_PRIVILEGE);
  });

  test('o que a aplicação precisa continua permitido (colunas mutáveis)', async () => {
    expect(await sqlState(`UPDATE wallets SET balance = balance, version = version, updated_at = updated_at WHERE id = '${uuid()}'`)).toBeUndefined();
    expect(await sqlState(`UPDATE outbox_messages SET published_at = published_at WHERE id = '${uuid()}'`)).toBeUndefined();
    expect(await sqlState(`SELECT id FROM wallets WHERE id = '${uuid()}' FOR NO KEY UPDATE`)).toBeUndefined();
  });
});
