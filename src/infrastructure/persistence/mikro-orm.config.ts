import { defineConfig } from '@mikro-orm/postgresql';
import { Migrator } from '@mikro-orm/migrations';
import { appDatabaseUrl } from './database-urls';
import { migrationsList } from './migrations';
import { resilientDriverOptions } from './pg-pool-resilience';
import { entities } from './schemas';

export interface OrmConfigOverrides {
  clientUrl?: string;
  poolMax?: number;
  /** Teto para abrir conexão (DB_CONNECT_TIMEOUT_MS). */
  connectTimeoutMs?: number;
  /** Teto para esperar vaga no pool (DB_POOL_ACQUIRE_TIMEOUT_MS). */
  acquireTimeoutMs?: number;
  /** Watchdog de query sem resposta (DB_QUERY_TIMEOUT_MS); 0 desliga (migrations). */
  queryTimeoutMs?: number;
  /** application_name no Postgres: identifica réplicas, testes e migrations em pg_stat_activity. */
  applicationName?: string;
  onConnectionError?: (error: Error, state: 'idle' | 'active') => void;
}

const envNumber = (name: string, fallback: number) => {
  const raw = process.env[name];
  return raw === undefined || raw === '' ? fallback : Number(raw);
};

export function buildOrmConfig(overrides: OrmConfigOverrides = {}) {
  const statementTimeoutMs = envNumber('DB_STATEMENT_TIMEOUT_MS', 15_000);
  return defineConfig({
    clientUrl: overrides.clientUrl ?? appDatabaseUrl(),
    entities,
    extensions: [Migrator],
    pool: { min: 0, max: overrides.poolMax ?? Number(process.env.DB_POOL_MAX ?? 10) },
    driverOptions: resilientDriverOptions({
      connectTimeoutMs: overrides.connectTimeoutMs ?? envNumber('DB_CONNECT_TIMEOUT_MS', 5_000),
      acquireTimeoutMs: overrides.acquireTimeoutMs ?? envNumber('DB_POOL_ACQUIRE_TIMEOUT_MS', 30_000),
      queryTimeoutMs: overrides.queryTimeoutMs ?? envNumber('DB_QUERY_TIMEOUT_MS', statementTimeoutMs + 3_000),
      applicationName: overrides.applicationName ?? 'wagering',
      ...(overrides.onConnectionError ? { onConnectionError: overrides.onConnectionError } : {}),
    }),
    // Schema é responsabilidade exclusiva das migrations versionadas.
    ensureDatabase: false,
    allowGlobalContext: false,
    debug: false,
    migrations: {
      migrationsList,
      tableName: 'mikro_orm_migrations',
      transactional: true,
      allOrNothing: true,
      snapshot: false,
    },
  });
}
