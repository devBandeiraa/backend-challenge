import { MikroORM } from '@mikro-orm/postgresql';
import { appDatabaseUrl, migrationDatabaseUrl } from './database-urls';
import { APP_PRIVILEGES_ROLE } from './migrations/008-app-role-least-privilege';
import { buildOrmConfig } from './mikro-orm.config';

/**
 * Migrations rodam com o OWNER do schema (MIGRATION_DATABASE_URL). A aplicação conecta com outro
 * usuário (DATABASE_URL), sem superuser nem ownership, membro do papel de privilégios da migration 008.
 */
export async function migrate(
  command: 'up' | 'down' | 'reset',
  ownerUrl: string = migrationDatabaseUrl(),
  appUrl: string = appDatabaseUrl(),
): Promise<void> {
  const orm = await MikroORM.init(buildOrmConfig({ clientUrl: ownerUrl, poolMax: 2 }));
  try {
    if (command === 'up' || command === 'reset') {
      if (command === 'reset') await orm.migrator.down({ to: 0 });
      await orm.migrator.up();
      await ensureAppLogin(orm, appUrl);
    } else {
      await orm.migrator.down();
    }
  } finally {
    await orm.close(true);
  }
}

/**
 * Garante o usuário de login da aplicação (nome e senha vindos de DATABASE_URL), sem privilégios
 * próprios: tudo vem da herança do papel da migration 008. Idempotente: cria ou realinha.
 * A credencial é configuração operacional, por isso não fica numa migration versionada.
 */
async function ensureAppLogin(orm: MikroORM, appUrl: string): Promise<void> {
  const url = new URL(appUrl);
  const user = decodeURIComponent(url.username);
  const password = decodeURIComponent(url.password);
  const em = orm.em.fork();
  const [owner] = await em.execute<{ current_user: string }[]>('SELECT current_user');
  if (!user || user === owner?.current_user) {
    throw new Error('DATABASE_URL must use a dedicated application user, not the schema owner (MIGRATION_DATABASE_URL)');
  }
  const role = quoteIdent(user);
  const attributes = `LOGIN PASSWORD ${quoteLiteral(password)} NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS INHERIT`;
  const exists = await em.execute<unknown[]>('SELECT 1 FROM pg_roles WHERE rolname = ?', [user]);
  await em.execute(exists.length > 0 ? `ALTER ROLE ${role} ${attributes}` : `CREATE ROLE ${role} ${attributes}`);
  await em.execute(`GRANT ${APP_PRIVILEGES_ROLE} TO ${role}`);
}

const quoteIdent = (value: string) => `"${value.replaceAll('"', '""')}"`;
const quoteLiteral = (value: string) => `'${value.replaceAll("'", "''")}'`;

if (import.meta.main) {
  const command = (process.argv[2] ?? 'up') as 'up' | 'down' | 'reset';
  await migrate(command);
  console.log(`migration ${command}: ok`);
}
