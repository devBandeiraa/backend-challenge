/**
 * Erros de CONEXÃO reais (sem mock): a conexão do Postgres da aplicação é derrubada com pg_terminate_backend
 * enquanto ela espera o lock da wallet (segurado por outra transação). O cliente vê 57P01 / "Connection
 * terminated" / "not queryable", e cada caminho precisa tratá-lo como transitório:
 *  - HTTP: 503 + Retry-After (nunca 500); o reenvio com a mesma key processa uma vez;
 *  - consumer SQS: retry (sem DLQ, sem FAILED); a mensagem é processada depois;
 *  - worker de PENDING_REFERENCE: a linha continua pendente (NUNCA FAILED) e é resolvida depois.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from 'bun:test';
import { MikroORM } from '@mikro-orm/postgresql';
import type { TransactionScope, UnitOfWork } from '../../src/application/ports';
import { ProcessWagerTransactionUseCase } from '../../src/application/wagering/process-wager-transaction';
import { ResolvePendingReferencesUseCase } from '../../src/application/wagering/resolve-pending-references';
import { ExponentialBackoff } from '../../src/domain/messaging/backoff-policy';
import { AppMetrics } from '../../src/infrastructure/observability/metrics';
import { isTransientDbError } from '../../src/infrastructure/persistence/pg-errors';
import { SystemClock, UuidV7Generator } from '../../src/infrastructure/system';
import { createLogger } from '../../src/infrastructure/observability/logger';
import { buildOrmConfig } from '../../src/infrastructure/persistence/mikro-orm.config';
import { LOCK_WAIT_MARKER } from '../../src/infrastructure/persistence/pg-pool-resilience';
import { WagerTransactionConsumer } from '../../src/interfaces/sqs/wager-transaction.consumer';
import { type AppContext, createAppContext, countRows, messageData, openTestWallet, wagerCommand, walletBalance } from '../support/app-context';
import { createOwnerOrm, TEST_DATABASE_URL } from '../support/db';
import { http, startHttpApp } from '../support/http-app';
import { ledgerInvariantAfterEach } from '../support/invariants';
import { createTestQueues, wagerMessage, waitFor } from '../support/sqs';

let app: AppContext;
let owner: MikroORM;
const touch = ledgerInvariantAfterEach(() => app.orm);
const logger = createLogger({ test: 'connection-failures' });
logger.level = 'silent';
const cleanups: (() => Promise<unknown>)[] = [];

beforeAll(async () => {
  // lock_timeout longo: a espera dura o suficiente para derrubarmos a conexão no meio dela
  app = await createAppContext({ poolMax: 4, uow: { maxAttempts: 2, lockTimeoutMs: 20_000, statementTimeoutMs: 40_001 } });
  owner = await createOwnerOrm(3);
});
afterEach(async () => {
  for (const c of cleanups.splice(0)) await c();
});
afterAll(async () => {
  await app.close();
  await owner.close(true);
});

/** Outra transação segura a wallet até `release()` (a espera da app fica no lock isento). */
async function holdWallet(walletId: string) {
  let release!: () => void;
  const released = new Promise<void>((r) => (release = r));
  let locked!: () => void;
  const isLocked = new Promise<void>((r) => (locked = r));
  const holder = owner.em.fork().transactional(async (em) => {
    await em.execute(`SELECT id FROM wallets WHERE id = ? FOR NO KEY UPDATE`, [walletId]); // o mesmo lock da app: o INSERT passa e a espera fica no lockById
    locked();
    await released;
  });
  await isLocked;
  const done = async () => {
    release();
    await holder;
  };
  cleanups.push(done);
  return done;
}

/** Derruba (pg_terminate_backend) a conexão que está esperando o lock da wallet. Devolve quantas derrubou. */
async function terminateLockWaiter(): Promise<number> {
  const rows = await waitFor(async () => {
    const found = await owner.em.fork().execute<{ pid: number }[]>(
      `SELECT pid FROM pg_stat_activity
        WHERE pid <> pg_backend_pid() AND wait_event_type = 'Lock' AND query LIKE ?`,
      [`%${LOCK_WAIT_MARKER}%`],
    );
    return found.length > 0 ? found : undefined;
  }, 15_000, 50);
  for (const { pid } of rows) await owner.em.fork().execute(`SELECT pg_terminate_backend(?)`, [pid]);
  return rows.length;
}

describe('conexão derrubada no meio da transação', () => {
  test('HTTP: 503 com Retry-After (nunca 500); o reenvio com a mesma key processa uma única vez', async () => {
    const server = await startHttpApp({ DB_LOCK_TIMEOUT_MS: '20000', DB_STATEMENT_TIMEOUT_MS: '40001', DB_QUERY_TIMEOUT_MS: '45000', UOW_MAX_ATTEMPTS: '2' });
    cleanups.push(server.close);
    const wallet = (await http(server.baseUrl, 'POST', '/wallets', { body: { playerId: crypto.randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } } })).body;
    touch(wallet.id);
    const release = await holdWallet(wallet.id);

    const body = { providerId: 'provider-a', externalTransactionId: `tx-${crypto.randomUUID()}`, playerId: wallet.playerId, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '10.00', currency: 'BRL' } };
    const headers = { 'idempotency-key': `provider-a:${body.externalTransactionId}` };
    const pending = http(server.baseUrl, 'POST', '/wagering/transactions', { body, headers });
    expect(await terminateLockWaiter()).toBe(1);
    const res = await pending;

    expect(res.status).toBe(503);
    expect(res.headers.get('retry-after')).toBe('1');
    expect(res.body.error.code).toBe('TRANSIENT_FAILURE');
    await release();

    const retry = await http(server.baseUrl, 'POST', '/wagering/transactions', { body, headers });
    expect(retry.status).toBe(200);
    expect(retry.body).toMatchObject({ status: 'PROCESSED', balance: { amount: '90.00' }, idempotentReplay: false });
    expect(await countRows(app.orm, `wallet_ledger_entries WHERE wallet_id = ? AND direction = 'DEBIT'`, [wallet.id])).toBe(1);
    // a instância segue saudável: o pool descartou o cliente quebrado
    expect((await http(server.baseUrl, 'GET', '/health/ready')).status).toBe(200);
  }, 60_000);

  test('consumer SQS: retry (sem DLQ e sem FAILED); a mensagem é processada depois', async () => {
    const { gateway } = await createTestQueues('conn-failure');
    const consumer = new WagerTransactionConsumer(gateway, app.processTransaction, app.metrics, logger, {
      consumerName: 'conn-failure-consumer',
      maxAttempts: 5,
      waitTimeSeconds: 1,
      visibilityTimeoutSeconds: 60,
      batchSize: 10,
      shutdownGraceMs: 10_000,
      retryBackoff: new ExponentialBackoff({ baseMs: 1_000, factor: 1, maxMs: 1_000, jitterRatio: 0 }),
    });
    const w = await openTestWallet(app, '100.00');
    touch(w.id);
    const release = await holdWallet(w.id);
    const cmd = wagerCommand(w);
    await gateway.sendToInput(wagerMessage(messageData(cmd)).body, w.id, crypto.randomUUID());
    consumer.start();
    cleanups.push(() => consumer.stop());

    expect(await terminateLockWaiter()).toBe(1);
    await waitFor(async () => ((await app.metrics.sqsRetries.get()).values[0]?.value ?? 0) >= 1 || undefined, 15_000, 50);
    expect(await countRows(app.orm, `wager_transactions WHERE idempotency_key = ?`, [cmd.idempotencyKey])).toBe(0); // nada gravado, nada FAILED
    const dlq = (await app.metrics.dlq.get()).values.reduce((n, v) => n + v.value, 0);
    expect(dlq).toBe(0);
    await release();

    const row = await waitFor(async () => {
      const [r] = await app.orm.em.fork().execute<{ status: string }[]>(`SELECT status FROM wager_transactions WHERE idempotency_key = ?`, [cmd.idempotencyKey]);
      return r?.status === 'PROCESSED' ? r : undefined;
    }, 20_000, 100);
    expect(row.status).toBe('PROCESSED');
    expect(await walletBalance(app.orm, w.id)).toBe('90.00');
  }, 60_000);

  test('worker de PENDING_REFERENCE: a linha continua pendente (nunca FAILED) e é resolvida depois', async () => {
    const w = await openTestWallet(app, '100.00');
    touch(w.id);
    // REFUND antes da BET: PENDING_REFERENCE; depois a BET chega e a pendência vence
    const bet = wagerCommand(w, { money: { amount: '30.00', currency: 'BRL' } });
    const refund = wagerCommand(w, { kind: 'REFUND', money: { amount: '30.00', currency: 'BRL' }, referenceExternalTransactionId: bet.externalTransactionId });
    expect((await app.processTransaction.execute(refund)).status).toBe('PENDING_REFERENCE');
    expect((await app.processTransaction.execute(bet)).status).toBe('PROCESSED');
    // vencida antes de qualquer outra pendência do banco: é a que o worker reivindica primeiro
    await owner.em.fork().execute(`UPDATE wager_transactions SET next_attempt_at = '-infinity' WHERE idempotency_key = ?`, [refund.idempotencyKey]);

    const release = await holdWallet(w.id);
    const run = app.resolvePending.runOnce(1).then(
      () => 'resolved',
      () => 'failed-transiently',
    );
    expect(await terminateLockWaiter()).toBe(1);
    expect(await run).toBe('failed-transiently');
    const [after] = await app.orm.em.fork().execute<{ status: string; failure_code: string | null }[]>(
      `SELECT status, failure_code FROM wager_transactions WHERE idempotency_key = ?`,
      [refund.idempotencyKey],
    );
    expect(after).toEqual({ status: 'PENDING_REFERENCE', failure_code: null });
    await release();

    await app.resolvePending.runOnce(1);
    const [resolved] = await app.orm.em.fork().execute<{ status: string }[]>(`SELECT status FROM wager_transactions WHERE idempotency_key = ?`, [refund.idempotencyKey]);
    expect(resolved?.status).toBe('PROCESSED');
    expect(await walletBalance(app.orm, w.id)).toBe('100.00');
  }, 60_000);
});

/**
 * Derruba a PRÓPRIA conexão da transação ENTRE duas instruções (antes do lockById) e espera o cliente perceber.
 * A instrução seguinte falha SEM SQLSTATE ("Client has encountered a connection error and is not queryable"):
 * exatamente o erro que, na v1.0.0, era classificado como bug (HTTP 500) e podia virar FAILED no worker.
 */
function breakConnectionBeforeLock(real: UnitOfWork) {
  const state = { armed: true, rawErrors: [] as string[] };
  const uow: UnitOfWork = {
    run: (work) =>
      real.run(async (scope: TransactionScope) => {
        const wallets = scope.wallets;
        const lockById = async (id: string) => {
          if (state.armed) {
            state.armed = false;
            const em = (wallets as unknown as { em: { execute(sql: string): Promise<{ pid: number }[]> } }).em;
            const [row] = await em.execute('SELECT pg_backend_pid() AS pid');
            await owner.em.fork().execute('SELECT pg_terminate_backend(?)', [row!.pid]);
            await Bun.sleep(300);
          }
          try {
            return await wallets.lockById(id);
          } catch (error) {
            state.rawErrors.push(String((error as Error).message));
            throw error;
          }
        };
        const proxied = new Proxy(wallets, {
          get: (target, prop) => (prop === 'lockById' ? lockById : Reflect.get(target, prop)),
        });
        return work({ ...scope, wallets: proxied });
      }),
  };
  return { uow, state };
}

describe('erro de conexão SEM SQLSTATE ("not queryable") entre instruções', () => {
  const NOT_QUERYABLE = /not queryable|Connection terminated/;

  test('HTTP: 503 + Retry-After, nunca 500; o reenvio processa uma única vez', async () => {
    let injected!: ReturnType<typeof breakConnectionBeforeLock>['state'];
    const server = await startHttpApp({
      patchUnitOfWork: (uow) => {
        const { uow: broken, state } = breakConnectionBeforeLock({ run: uow.run.bind(uow) });
        injected = state;
        uow.run = broken.run;
      },
    });
    cleanups.push(server.close);
    const wallet = (await http(server.baseUrl, 'POST', '/wallets', { body: { playerId: crypto.randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } } })).body;
    touch(wallet.id);
    const body = { providerId: 'provider-a', externalTransactionId: `tx-${crypto.randomUUID()}`, playerId: wallet.playerId, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '10.00', currency: 'BRL' } };
    const headers = { 'idempotency-key': `provider-a:${body.externalTransactionId}` };

    const res = await http(server.baseUrl, 'POST', '/wagering/transactions', { body, headers });
    expect(injected.rawErrors.some((m) => NOT_QUERYABLE.test(m))).toBe(true);
    expect(res.status).toBe(503);
    expect(res.headers.get('retry-after')).toBe('1');
    expect(res.body.error.code).toBe('TRANSIENT_FAILURE');

    const retry = await http(server.baseUrl, 'POST', '/wagering/transactions', { body, headers });
    expect(retry.status).toBe(200);
    expect(retry.body).toMatchObject({ status: 'PROCESSED', balance: { amount: '90.00' }, idempotentReplay: false });
    expect(await countRows(app.orm, `wallet_ledger_entries WHERE wallet_id = ? AND direction = 'DEBIT'`, [wallet.id])).toBe(1);
  }, 60_000);

  test('consumer SQS: retry (sem DLQ, sem FAILED) e processamento na entrega seguinte', async () => {
    const { gateway } = await createTestQueues('not-queryable');
    const { uow, state } = breakConnectionBeforeLock(app.uow);
    const useCase = new ProcessWagerTransactionUseCase(uow, new SystemClock(), new UuidV7Generator());
    const metrics = new AppMetrics();
    const consumer = new WagerTransactionConsumer(gateway, useCase, metrics, logger, {
      consumerName: 'not-queryable-consumer',
      maxAttempts: 5,
      waitTimeSeconds: 1,
      visibilityTimeoutSeconds: 60,
      batchSize: 10,
      shutdownGraceMs: 10_000,
      retryBackoff: new ExponentialBackoff({ baseMs: 1_000, factor: 1, maxMs: 1_000, jitterRatio: 0 }),
    });
    const w = await openTestWallet(app, '100.00');
    touch(w.id);
    const cmd = wagerCommand(w);
    await gateway.sendToInput(wagerMessage(messageData(cmd)).body, w.id, crypto.randomUUID());
    consumer.start();
    cleanups.push(() => consumer.stop());

    const row = await waitFor(async () => {
      const [r] = await app.orm.em.fork().execute<{ status: string }[]>(`SELECT status FROM wager_transactions WHERE idempotency_key = ?`, [cmd.idempotencyKey]);
      return r?.status === 'PROCESSED' ? r : undefined;
    }, 20_000, 100);
    expect(row.status).toBe('PROCESSED');
    expect(state.rawErrors.some((m) => NOT_QUERYABLE.test(m))).toBe(true);
    // o consumer registra o desfecho DEPOIS do commit e do ack: a linha PROCESSED aparece no banco antes da métrica
    const outcomes = await waitFor(async () => {
      const values = Object.fromEntries((await metrics.sqsMessages.get()).values.map((v) => [v.labels.outcome, v.value]));
      return values.processed === 1 ? values : undefined;
    }, 5_000, 20);
    expect(outcomes).toEqual({ retry: 1, processed: 1 });
    expect((await metrics.dlq.get()).values).toEqual([]);
    expect(await walletBalance(app.orm, w.id)).toBe('90.00');
  }, 60_000);

  test('worker de PENDING_REFERENCE: a linha continua pendente (nunca FAILED) e é resolvida depois', async () => {
    const w = await openTestWallet(app, '100.00');
    touch(w.id);
    const bet = wagerCommand(w, { money: { amount: '30.00', currency: 'BRL' } });
    const refund = wagerCommand(w, { kind: 'REFUND', money: { amount: '30.00', currency: 'BRL' }, referenceExternalTransactionId: bet.externalTransactionId });
    expect((await app.processTransaction.execute(refund)).status).toBe('PENDING_REFERENCE');
    expect((await app.processTransaction.execute(bet)).status).toBe('PROCESSED');
    await owner.em.fork().execute(`UPDATE wager_transactions SET next_attempt_at = '-infinity' WHERE idempotency_key = ?`, [refund.idempotencyKey]);

    const { uow, state } = breakConnectionBeforeLock(app.uow);
    const worker = new ResolvePendingReferencesUseCase(uow, new SystemClock(), new UuidV7Generator(), undefined, isTransientDbError);
    await expect(worker.runOnce(1)).rejects.toBeDefined();
    expect(state.rawErrors.some((m) => NOT_QUERYABLE.test(m))).toBe(true);
    const [after] = await app.orm.em.fork().execute<{ status: string; failure_code: string | null }[]>(
      `SELECT status, failure_code FROM wager_transactions WHERE idempotency_key = ?`,
      [refund.idempotencyKey],
    );
    expect(after).toEqual({ status: 'PENDING_REFERENCE', failure_code: null });

    await worker.runOnce(1); // desarmado: resolve normalmente
    const [resolved] = await app.orm.em.fork().execute<{ status: string }[]>(`SELECT status FROM wager_transactions WHERE idempotency_key = ?`, [refund.idempotencyKey]);
    expect(resolved?.status).toBe('PROCESSED');
    expect(await walletBalance(app.orm, w.id)).toBe('100.00');
  }, 60_000);
});


/**
 * Contrato de que depende o contorno de pg-pool-resilience.ts (limitação documentada no ARCHITECTURE §15):
 *  (a) o cliente `pg` expõe o campo INTERNO `_queryable`, `true` com a conexão viva e `false` depois que ela cai;
 *  (b) nesse cliente quebrado, o guard transforma ROLLBACK/BEGIN em no-op e NUNCA simula COMMIT.
 * Se uma atualização do pg renomear o campo, ou o guard deixar de agir, este teste falha antes de a réplica voltar a
 * vazar conexões numa queda real do Postgres (cenário coberto pelo e2e "queda do Postgres").
 */
describe('contrato do contorno do ROLLBACK em conexão morta (pg + guard)', () => {
  test('_queryable vira false quando a conexão cai; ROLLBACK/BEGIN resolvem, COMMIT rejeita', async () => {
    const orm = await MikroORM.init(buildOrmConfig({ clientUrl: TEST_DATABASE_URL, poolMax: 1, applicationName: 'wagering-test-contract' }));
    type GuardedClient = { _queryable?: boolean; query(sql: string): Promise<{ command?: string }>; release(error?: Error): void };
    try {
      const pool = (await (orm.em.getConnection() as unknown as { getNativeClient(): Promise<unknown> }).getNativeClient()) as {
        connect(): Promise<GuardedClient>;
      };
      const client = await pool.connect(); // novo cliente: passa pelo 'connect' do pool, onde o guard é instalado
      const { rows } = (await client.query('SELECT pg_backend_pid() AS pid')) as unknown as { rows: { pid: number }[] };
      expect(client._queryable).toBe(true); // (a) o campo existe e reflete a conexão viva

      await owner.em.fork().execute('SELECT pg_terminate_backend(?)', [rows[0]!.pid]);
      await waitFor(async () => client._queryable === false || undefined, 5_000, 20); // (a) e cai para false

      // (b) no-op nos comandos que o Kysely precisa ver bem-sucedidos para devolver a conexão ao pool
      expect((await client.query('ROLLBACK')).command).toBe('ROLLBACK');
      expect((await client.query('start transaction isolation level read committed')).command).toBe('START');
      // COMMIT jamais é simulado: confirmar sem o servidor seria mentir sobre durabilidade
      await expect(client.query('COMMIT')).rejects.toThrow(/not queryable/);
      client.release(new Error('broken connection'));
    } finally {
      await orm.close(true);
    }
  }, 30_000);
});
