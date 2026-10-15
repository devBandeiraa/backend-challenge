import { MikroORM } from '@mikro-orm/postgresql';
import { buildOrmConfig } from '../../src/infrastructure/persistence/mikro-orm.config';
import { migrate } from '../../src/infrastructure/persistence/migrate';

export const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ?? process.env.DATABASE_URL ?? 'postgresql://wagering:wagering@localhost:5442/wagering';

let migrated: Promise<void> | undefined;

/** Aplica as migrations pendentes uma vez por processo de teste (idempotente). */
export function ensureMigrated(): Promise<void> {
  migrated ??= migrate('up', TEST_DATABASE_URL);
  return migrated;
}

export async function createTestOrm(poolMax = 20): Promise<MikroORM> {
  await ensureMigrated();
  return MikroORM.init(buildOrmConfig({ clientUrl: TEST_DATABASE_URL, poolMax }));
}
