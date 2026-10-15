/**
 * Processo SEPARADO que consome a fila e morre (exit 137, como um SIGKILL/OOM) logo DEPOIS do
 * commit e ANTES do ack. Usado por test/concurrency/crash-after-commit.test.ts.
 * Env: SQS_INPUT_QUEUE, SQS_DLQ, SQS_EVENTS_QUEUE, CONSUMER_VISIBILITY_SECONDS.
 */
import { createLogger } from '../../src/infrastructure/observability/logger';
import { createSqsClient } from '../../src/infrastructure/messaging/sqs/sqs-config';
import { SqsGateway } from '../../src/infrastructure/messaging/sqs/sqs-gateway';
import { WagerTransactionConsumer } from '../../src/interfaces/sqs/wager-transaction.consumer';
import { createAppContext } from './app-context';

const app = await createAppContext({ poolMax: 2 });
const gateway = await SqsGateway.connect(createSqsClient(), {
  input: process.env.SQS_INPUT_QUEUE!,
  deadLetter: process.env.SQS_DLQ!,
  events: process.env.SQS_EVENTS_QUEUE!,
});
const logger = createLogger({ process: 'crashing-consumer' });

const consumer = new WagerTransactionConsumer(gateway, app.processTransaction, app.metrics, logger, {
  consumerName: 'test-consumer',
  maxAttempts: 5,
  waitTimeSeconds: 1,
  visibilityTimeoutSeconds: Number(process.env.CONSUMER_VISIBILITY_SECONDS ?? 3),
  batchSize: 1,
  shutdownGraceMs: 1_000,
  hooks: {
    afterCommit: (_message, outcome) => {
      // Commit confirmado no Postgres; o DeleteMessage nunca acontece.
      console.log(JSON.stringify({ event: 'committed-then-crash', transactionId: outcome.transactionId }));
      process.exit(137);
    },
  },
});
consumer.start();
