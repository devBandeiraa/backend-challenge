/**
 * Processo SEPARADO do publisher da outbox (mesmas classes da app, Postgres e SQS reais).
 * Usado por test/recovery/outbox-crash.test.ts. MODE:
 *  - crash-before-send: reivindica o lote (FOR UPDATE SKIP LOCKED), avisa "claimed" e trava ANTES do
 *                       SendMessage, segurando as linhas; o teste o mata com SIGKILL;
 *  - crash-after-send:  envia ao SQS, avisa "sent" e trava ANTES do COMMIT do published_at; SIGKILL;
 *  - publish:           publica até as ids de EXPECT_IDS estarem publicadas, avisa "published" e sai.
 * Env: MODE, SQS_INPUT_QUEUE, SQS_DLQ, SQS_EVENTS_QUEUE, EXPECT_IDS (publish).
 */
import { MikroORM } from '@mikro-orm/postgresql';
import { createSqsClient } from '../../src/infrastructure/messaging/sqs/sqs-config';
import { SqsGateway } from '../../src/infrastructure/messaging/sqs/sqs-gateway';
import { createLogger } from '../../src/infrastructure/observability/logger';
import { AppMetrics } from '../../src/infrastructure/observability/metrics';
import { buildOrmConfig } from '../../src/infrastructure/persistence/mikro-orm.config';
import { OutboxPublisher } from '../../src/infrastructure/workers/outbox-publisher';
import { TEST_DATABASE_URL } from './db';

const mode = process.env.MODE as 'crash-before-send' | 'crash-after-send' | 'publish';
const orm = await MikroORM.init(buildOrmConfig({ clientUrl: TEST_DATABASE_URL, poolMax: 2 }));
const gateway = await SqsGateway.connect(createSqsClient(), {
  input: process.env.SQS_INPUT_QUEUE!,
  deadLetter: process.env.SQS_DLQ!,
  events: process.env.SQS_EVENTS_QUEUE!,
});
const logger = createLogger({ process: `outbox-${mode}` });
logger.level = 'silent';

const announce = (event: string, ids: string[]) => console.log(JSON.stringify({ event, ids }));
/** Trava para sempre: a transação (e os locks das linhas) só termina quando o processo morre. */
const hang = () => new Promise<never>(() => undefined);

const publisher = new OutboxPublisher(orm, gateway, new AppMetrics(), logger, {
  batchSize: 100,
  hooks: {
    beforeSend:
      mode === 'crash-before-send'
        ? async (ids) => {
            announce('claimed', ids);
            await hang();
          }
        : undefined,
    afterSend:
      mode === 'crash-after-send'
        ? async (ids) => {
            announce('sent', ids);
            await hang();
          }
        : undefined,
  },
});

if (mode === 'publish') {
  const expected = (process.env.EXPECT_IDS ?? '').split(',').filter(Boolean);
  for (;;) {
    await publisher.publishOnce();
    const [row] = await orm.em
      .fork()
      .execute<{ n: number }[]>(`SELECT count(*)::int AS n FROM outbox_messages WHERE id = ANY(?::uuid[]) AND published_at IS NULL`, [
        `{${expected.join(',')}}`,
      ]);
    if (row!.n === 0) break;
    await Bun.sleep(100);
  }
  announce('published', expected);
  await orm.close(true);
  process.exit(0);
} else {
  await publisher.publishOnce(); // trava no hook até o SIGKILL
}
