import type { MikroORM } from '@mikro-orm/postgresql';
import {
  ProcessWagerTransactionUseCase,
  type SubmitWagerTransactionCommand,
} from '../../src/application/wagering/process-wager-transaction';
import { ResolvePendingReferencesUseCase } from '../../src/application/wagering/resolve-pending-references';
import type { PendingReferencePolicy } from '../../src/application/wagering/settle-transaction';
import { OpenWalletUseCase } from '../../src/application/wallets/open-wallet';
import { ExponentialBackoff } from '../../src/domain/messaging/backoff-policy';
import { AppMetrics } from '../../src/infrastructure/observability/metrics';
import { MikroOrmUnitOfWork, type UnitOfWorkOptions } from '../../src/infrastructure/persistence/mikro-orm-unit-of-work';
import { SystemClock, UuidV7Generator } from '../../src/infrastructure/system';
import { createTestOrm } from './db';

export interface AppContextOptions {
  poolMax?: number;
  uow?: UnitOfWorkOptions;
  pending?: Partial<{ maxAttempts: number; baseMs: number; maxMs: number }>;
}

/**
 * Casos de uso reais sobre Postgres real (nada mockado). Cada contexto tem seu PRÓPRIO
 * MikroORM/pool, como uma instância separada da aplicação.
 */
export async function createAppContext(options: AppContextOptions | number = {}) {
  const opts: AppContextOptions = typeof options === 'number' ? { poolMax: options } : options;
  const orm = await createTestOrm(opts.poolMax ?? 20);
  const metrics = new AppMetrics();
  const uow = new MikroOrmUnitOfWork(orm, opts.uow, {
    onRetry: ({ sqlState }) => metrics.lockConflicts.inc({ sqlstate: sqlState ?? 'unknown' }),
    onExhausted: () => metrics.retriesExhausted.inc(),
  });
  const clock = new SystemClock();
  const ids = new UuidV7Generator();
  const pending: PendingReferencePolicy = {
    maxAttempts: opts.pending?.maxAttempts ?? 10,
    backoff: new ExponentialBackoff({
      baseMs: opts.pending?.baseMs ?? 500,
      factor: 2,
      maxMs: opts.pending?.maxMs ?? 30_000,
      jitterRatio: 0,
    }),
  };
  return {
    orm,
    uow,
    metrics,
    openWallet: new OpenWalletUseCase(uow, clock, ids),
    processTransaction: new ProcessWagerTransactionUseCase(uow, clock, ids, pending),
    resolvePending: new ResolvePendingReferencesUseCase(uow, clock, ids, pending),
    close: () => orm.close(true),
  };
}

export type AppContext = Awaited<ReturnType<typeof createAppContext>>;

export function wagerCommand(
  wallet: { id: string; playerId: string },
  overrides: Partial<SubmitWagerTransactionCommand> = {},
): SubmitWagerTransactionCommand {
  const providerId = overrides.providerId ?? 'provider-a';
  const externalTransactionId = overrides.externalTransactionId ?? `tx-${crypto.randomUUID()}`;
  return {
    providerId,
    externalTransactionId,
    idempotencyKey: overrides.idempotencyKey ?? `${providerId}:${externalTransactionId}`,
    playerId: wallet.playerId,
    walletId: wallet.id,
    roundId: 'round-1',
    gameId: 'fortune-chimp',
    kind: 'BET',
    money: { amount: '10.00', currency: 'BRL' },
    correlationId: `corr-${crypto.randomUUID()}`,
    ...overrides,
  };
}

/** `data` de mensagem SQS equivalente a um comando (key dentro do data). */
export function messageData(command: SubmitWagerTransactionCommand): Record<string, unknown> {
  const { correlationId: _c, causationId: _k, ...data } = command;
  return data;
}

export async function openTestWallet(app: AppContext, amount = '100.00', currency = 'BRL') {
  return app.openWallet.execute({ playerId: crypto.randomUUID(), initialBalance: { amount, currency }, correlationId: 'test' });
}

/**
 * Invariante final de TODOS os testes: wallet.balance == saldo reconstruído pelo ledger,
 * e version == versão do último lançamento (ou 1 sem lançamentos). Calculado no banco,
 * com NUMERIC exato, sem passar por number.
 */
export async function assertLedgerInvariant(orm: MikroORM, walletId: string): Promise<void> {
  const [row] = await orm.em.fork().execute<{ stored: string; rebuilt: string; version: number; last_version: number | null }[]>(
    `SELECT w.balance::text AS stored,
            COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END), 0)::numeric(19,2)::text AS rebuilt,
            w.version,
            MAX(l.wallet_version) AS last_version
       FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id = w.id
      WHERE w.id = ?
      GROUP BY w.id`,
    [walletId],
  );
  if (!row) throw new Error(`wallet ${walletId} not found`);
  if (row.stored !== row.rebuilt) throw new Error(`ledger invariant broken: stored ${row.stored} != rebuilt ${row.rebuilt}`);
  if (row.version !== (row.last_version ?? 1)) {
    throw new Error(`version invariant broken: wallet v${row.version} != last ledger v${row.last_version}`);
  }
}

export async function countRows(orm: MikroORM, sql: string, params: unknown[]): Promise<number> {
  const [row] = await orm.em.fork().execute<{ n: string }[]>(`SELECT COUNT(*)::text AS n FROM ${sql}`, params);
  return Number(row?.n ?? 0);
}

export async function walletBalance(orm: MikroORM, walletId: string): Promise<string> {
  const [row] = await orm.em.fork().execute<{ b: string }[]>(`SELECT balance::text AS b FROM wallets WHERE id = ?`, [walletId]);
  return row!.b;
}

/** Barreira: N participantes só seguem quando todos chegaram (disparo realmente simultâneo). */
export function barrier(parties: number) {
  let arrived = 0;
  let release!: () => void;
  const gate = new Promise<void>((r) => (release = r));
  return async () => {
    if (++arrived === parties) release();
    await gate;
  };
}

/** Executa N tarefas que só começam depois que TODAS estão prontas na barreira. */
export async function inParallel<T>(n: number, task: (i: number) => Promise<T>): Promise<PromiseSettledResult<T>[]> {
  const ready = barrier(n);
  return Promise.allSettled(
    Array.from({ length: n }, async (_, i) => {
      await ready();
      return task(i);
    }),
  );
}
