/**
 * Worker morto DEPOIS do commit e ANTES do ack, num processo real separado (exit 137).
 * A mensagem volta após o visibility timeout; outra instância a recebe, a inbox reconhece o
 * messageId e nada é reaplicado. Postgres e LocalStack reais.
 */
import { afterAll, beforeAll, expect, test } from 'bun:test';
import type { SQSClient } from '@aws-sdk/client-sqs';
import { createLogger } from '../../src/infrastructure/observability/logger';
import { WagerTransactionConsumer } from '../../src/interfaces/sqs/wager-transaction.consumer';
import {
  type AppContext,
  assertLedgerInvariant,
  countRows,
  createAppContext,
  messageData,
  openTestWallet,
  wagerCommand,
  walletBalance,
} from '../support/app-context';
import { createTestQueues, drain, type SqsGateway, waitFor, wagerMessage } from '../support/sqs';

let app: AppContext;
let queues: Awaited<ReturnType<typeof createTestQueues>>;
let gateway: SqsGateway;
let client: SQSClient;
const logger = createLogger();
logger.level = 'silent';

beforeAll(async () => {
  app = await createAppContext({ poolMax: 5 });
  queues = await createTestQueues('crash');
  ({ gateway, client } = queues);
});
afterAll(async () => {
  await app.close();
});

test('commit confirmado, processo morre antes do ack → redelivery → inbox evita o segundo débito', async () => {
  const w = await openTestWallet(app, '100.00');
  const command = wagerCommand(w, { money: { amount: '30.00', currency: 'BRL' } });
  const msg = wagerMessage(messageData(command));
  await gateway.sendToInput(msg.body, w.id, crypto.randomUUID());

  const child = Bun.spawn(['bun', 'run', 'test/support/crashing-consumer.ts'], {
    env: {
      ...process.env,
      SQS_INPUT_QUEUE: queues.names.input,
      SQS_DLQ: queues.names.deadLetter,
      SQS_EVENTS_QUEUE: queues.names.events,
      CONSUMER_VISIBILITY_SECONDS: '3',
      LOG_LEVEL: 'silent',
    },
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const exitCode = await child.exited;
  const stdout = await new Response(child.stdout).text();

  // 1) o processo morreu depois do commit: o efeito está no banco...
  expect(exitCode).toBe(137);
  expect(stdout).toContain('committed-then-crash');
  expect(await walletBalance(app.orm, w.id)).toBe('70.00');
  expect(await countRows(app.orm, `inbox_messages WHERE message_id = ?`, [msg.messageId])).toBe(1);

  // 2) ...mas a mensagem NÃO foi apagada: outra instância a recebe após o visibility timeout.
  const survivor = new WagerTransactionConsumer(gateway, app.processTransaction, app.metrics, logger, {
    consumerName: 'test-consumer',
    maxAttempts: 5,
    waitTimeSeconds: 1,
    visibilityTimeoutSeconds: 30,
    batchSize: 10,
    shutdownGraceMs: 5_000,
  });
  survivor.start();
  await waitFor(async () => {
    const dup = (await app.metrics.duplicates.get()).values.find((v) => v.labels.layer === 'inbox');
    return (dup?.value ?? 0) >= 1 || undefined;
  }, 20_000);
  await survivor.stop();

  // 3) nenhum efeito duplicado; mensagem consumida; invariante preservada.
  expect(await walletBalance(app.orm, w.id)).toBe('70.00');
  expect(await countRows(app.orm, `wallet_ledger_entries WHERE wallet_id = ? AND direction = 'DEBIT'`, [w.id])).toBe(1);
  expect(await drain(client, gateway.urls.input, 500)).toHaveLength(0);
  expect(await drain(client, gateway.urls.deadLetter, 300)).toHaveLength(0);
  await assertLedgerInvariant(app.orm, w.id);
});
