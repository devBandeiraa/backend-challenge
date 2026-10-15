import { MikroORM } from '@mikro-orm/postgresql';
import { buildOrmConfig } from './mikro-orm.config';

export async function migrate(command: 'up' | 'down' | 'reset', clientUrl?: string): Promise<void> {
  const orm = await MikroORM.init(buildOrmConfig({ clientUrl, poolMax: 2 }));
  try {
    if (command === 'up' || command === 'reset') {
      if (command === 'reset') await orm.migrator.down({ to: 0 });
      await orm.migrator.up();
    } else {
      await orm.migrator.down();
    }
  } finally {
    await orm.close(true);
  }
}

if (import.meta.main) {
  const command = (process.argv[2] ?? 'up') as 'up' | 'down' | 'reset';
  await migrate(command);
  console.log(`migration ${command}: ok`);
}
