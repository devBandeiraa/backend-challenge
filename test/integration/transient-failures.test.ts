/**
 * Falha transitória REAL (sem mock): um lock segurado sobre a wallet por outra transação faz a
 * submissão estourar o lock_timeout (55P03). A unit of work refaz a transação inteira, incrementando
 * a métrica de conflito a cada retry; esgotadas as tentativas, o HTTP responde 503 com Retry-After.
 */
import { afterAll, beforeAll, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import { TransientInfrastructureError } from '../../src/application/errors';
import { MikroOrmUnitOfWork } from '../../src/infrastructure/persistence/mikro-orm-unit-of-work';
import { sqlStateOf } from '../../src/infrastructure/persistence/pg-errors';
import { assertLedgerInvariant } from '../support/app-context';
import { createTestOrm } from '../support/db';
import { http, startHttpApp } from '../support/http-app';

let orm: MikroORM;
let baseUrl: string;
let close: () => Promise<void>;
const MAX_ATTEMPTS = 3;

beforeAll(async () => {
  orm = await createTestOrm(3);
  ({ baseUrl, close } = await startHttpApp({ DB_LOCK_TIMEOUT_MS: '150', UOW_MAX_ATTEMPTS: String(MAX_ATTEMPTS) }));
});
afterAll(async () => {
  await close();
  await orm.close(true);
});

function lockConflicts(metrics: string): number {
  const line = metrics.split('\n').find((l) => l.startsWith('wagering_db_lock_conflicts_total{sqlstate="55P03"}'));
  return line ? Number(line.split(' ').at(-1)) : 0;
}

test('lock segurado além do lock_timeout → retries medidos → 503 com Retry-After; depois do release, sucesso', async () => {
  const wallet = (await http(baseUrl, 'POST', '/wallets', {
    body: { playerId: crypto.randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } },
  })).body as { id: string; playerId: string };

  let release!: () => void;
  const released = new Promise<void>((r) => (release = r));
  let locked!: () => void;
  const isLocked = new Promise<void>((r) => (locked = r));
  // Outra "instância" segura FOR UPDATE na wallet: conflita até com o FOR KEY SHARE da FK do insert.
  const holder = orm.em.fork().transactional(async (em) => {
    await em.execute(`SELECT id FROM wallets WHERE id = ? FOR UPDATE`, [wallet.id]);
    locked();
    await released;
  });
  await isLocked;

  const before = lockConflicts((await http(baseUrl, 'GET', '/metrics')).body);
  const body = {
    providerId: 'provider-a',
    externalTransactionId: `tx-${crypto.randomUUID()}`,
    playerId: wallet.playerId,
    walletId: wallet.id,
    roundId: 'r',
    gameId: 'g',
    kind: 'BET',
    money: { amount: '10.00', currency: 'BRL' },
  };
  const headers = { 'idempotency-key': `provider-a:${body.externalTransactionId}` };
  const res = await http(baseUrl, 'POST', '/wagering/transactions', { body, headers });

  expect(res.status).toBe(503);
  expect(res.headers.get('retry-after')).toBe('1');
  expect(res.body.error.code).toBe('TRANSIENT_FAILURE');
  const after = lockConflicts((await http(baseUrl, 'GET', '/metrics')).body);
  expect(after - before).toBe(MAX_ATTEMPTS - 1); // um incremento por retry

  release();
  await holder;

  // O 503 não deixou nada para trás: reenviar com a MESMA key processa normalmente (sem replay).
  const retry = await http(baseUrl, 'POST', '/wagering/transactions', { body, headers });
  expect(retry.status).toBe(200);
  expect(retry.body).toMatchObject({ status: 'PROCESSED', balance: { amount: '90.00' }, idempotentReplay: false });
  await assertLedgerInvariant(orm, wallet.id);
});

/** EntityManager da transação corrente (os repositórios o guardam): só para executar SQL arbitrário no teste. */
const emOf = (scope: unknown) => (scope as { wallets: { em: { execute(sql: string): Promise<Record<string, string>[]> } } }).wallets.em;

test('statement_timeout: instrução travada é cancelada (57014) e vira falha transitória, sem retry na mesma entrega', async () => {
  let retries = 0;
  const uow = new MikroOrmUnitOfWork(orm, { maxAttempts: 3, lockTimeoutMs: 100, statementTimeoutMs: 300 }, { onRetry: () => void retries++ });
  const seen: string[] = [];
  const startedAt = performance.now();
  const error = await uow
    .run(async (scope) => {
      seen.push((await emOf(scope).execute('SHOW statement_timeout'))[0]!.statement_timeout!);
      await emOf(scope).execute('SELECT pg_sleep(2)');
    })
    .catch((e: unknown) => e);
  const elapsed = performance.now() - startedAt;

  expect(seen).toEqual(['300ms']); // SET LOCAL aplicado na própria transação da unit of work
  expect(error).toBeInstanceOf(TransientInfrastructureError);
  expect(sqlStateOf((error as TransientInfrastructureError).cause)).toBe('57014');
  expect(retries).toBe(0); // 57014 não é refeito: não multiplica o pior caso
  expect(elapsed).toBeLessThan(1_500);
});
