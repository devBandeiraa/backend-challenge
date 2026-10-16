import { MikroORM } from '@mikro-orm/postgresql';
import { appDatabaseUrl, migrationDatabaseUrl } from '../../src/infrastructure/persistence/database-urls';
import { buildOrmConfig } from '../../src/infrastructure/persistence/mikro-orm.config';
import { migrate } from '../../src/infrastructure/persistence/migrate';

/** Conexão da APLICAÇÃO (sem superuser, sem ownership): a mesma identidade que a app usa em produção. */
export const TEST_DATABASE_URL = process.env.TEST_DATABASE_URL ?? appDatabaseUrl();
/** Conexão do OWNER do schema: só para migrations e para testes que contornam as garantias de propósito. */
export const TEST_OWNER_DATABASE_URL = process.env.TEST_OWNER_DATABASE_URL ?? migrationDatabaseUrl();

let migrated: Promise<void> | undefined;

/** Aplica as migrations pendentes uma vez por processo de teste (idempotente). */
export function ensureMigrated(): Promise<void> {
  migrated ??= migrate('up', TEST_OWNER_DATABASE_URL, TEST_DATABASE_URL);
  return migrated;
}

export async function createTestOrm(poolMax = 20): Promise<MikroORM> {
  await ensureMigrated();
  return MikroORM.init(buildOrmConfig({ clientUrl: TEST_DATABASE_URL, poolMax, applicationName: 'wagering-test' }));
}

/** ORM com a identidade do owner (schema tests com SQL puro, divergência forçada). */
export async function createOwnerOrm(poolMax = 5): Promise<MikroORM> {
  await ensureMigrated();
  return MikroORM.init(buildOrmConfig({ clientUrl: TEST_OWNER_DATABASE_URL, poolMax, applicationName: 'wagering-test-owner' }));
}
