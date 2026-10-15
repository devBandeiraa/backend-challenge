import type { MikroORM } from '@mikro-orm/postgresql';
import { ProcessWagerTransactionUseCase, type SubmitWagerTransactionCommand } from '../../src/application/wagering/process-wager-transaction';
import { OpenWalletUseCase } from '../../src/application/wallets/open-wallet';
import { MikroOrmUnitOfWork } from '../../src/infrastructure/persistence/mikro-orm-unit-of-work';
import { SystemClock, UuidV7Generator } from '../../src/infrastructure/system';
import { createTestOrm } from './db';

/** Casos de uso reais sobre Postgres real (nada mockado). */
export async function createAppContext(poolMax = 20) {
  const orm = await createTestOrm(poolMax);
  const uow = new MikroOrmUnitOfWork(orm);
  const clock = new SystemClock();
  const ids = new UuidV7Generator();
  return {
    orm,
    uow,
    openWallet: new OpenWalletUseCase(uow, clock, ids),
    processTransaction: new ProcessWagerTransactionUseCase(uow, clock, ids),
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
