// Timeout: passado por --timeout nos scripts (bun run test). setDefaultTimeout no preload só
// vale para o primeiro arquivo da execução, então não é usado aqui.
import { SQL } from 'bun';
import { APP_APPLICATION_NAME_PREFIX, appDatabaseUrl } from '../src/infrastructure/persistence/database-urls';

// Os testes do host medem a outbox e as pendências diretamente; réplicas da app conectadas ao MESMO banco
// teriam publishers/workers concorrendo pelas mesmas linhas e tornariam as medições inválidas.
// A pergunta vai ao próprio banco dos testes: as réplicas se identificam com application_name
// "wagering-app:<instância>". Não depende de porta (a 1ª versão olhava localhost:8088 e acusava qualquer
// stack nessa porta, inclusive de outro projeto) nem de nome de projeto do compose.
if (process.env.MULTI_INSTANCE !== '1') {
  const db = new SQL({ url: process.env.TEST_DATABASE_URL ?? appDatabaseUrl(), max: 1, connectionTimeout: 3 });
  const replicas = await Promise.race([
    db`SELECT application_name FROM pg_stat_activity
        WHERE datname = current_database() AND application_name LIKE ${`${APP_APPLICATION_NAME_PREFIX}%`}
        GROUP BY application_name`.catch(() => [] as { application_name: string }[]),
    Bun.sleep(5_000).then(() => [] as { application_name: string }[]), // banco fora: os próprios testes vão acusar
  ]);
  await db.close().catch(() => undefined);
  if (replicas.length > 0) {
    throw new Error(
      `app replicas are connected to the test database (${replicas.map((r: { application_name: string }) => r.application_name).join(', ')}): ` +
        'stop them with "docker compose stop app nginx" (bun run test:multi does it)',
    );
  }
}

export {};
