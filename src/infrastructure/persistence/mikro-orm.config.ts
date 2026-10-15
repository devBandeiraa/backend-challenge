import { defineConfig } from '@mikro-orm/postgresql';
import { Migrator } from '@mikro-orm/migrations';
import { appDatabaseUrl } from './database-urls';
import { migrationsList } from './migrations';
import { entities } from './schemas';

export function buildOrmConfig(overrides: { clientUrl?: string; poolMax?: number } = {}) {
  return defineConfig({
    clientUrl: overrides.clientUrl ?? appDatabaseUrl(),
    entities,
    extensions: [Migrator],
    pool: { min: 0, max: overrides.poolMax ?? Number(process.env.DB_POOL_MAX ?? 10) },
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
