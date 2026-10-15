/**
 * Logs estruturados: toda linha carrega o contexto de rastreio (via AsyncLocalStorage) e nenhuma
 * carrega payload financeiro. Readiness e métricas.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import { DeleteQueueCommand } from '@aws-sdk/client-sqs';
import { createLogger } from '../../src/infrastructure/observability/logger';
import { WagerTransactionConsumer } from '../../src/interfaces/sqs/wager-transaction.consumer';
import { createAppContext, messageData, openTestWallet, wagerCommand } from '../support/app-context';
import { ledgerInvariantAfterEach } from '../support/invariants';
import { createTestOrm } from '../support/db';
import { http, parseLogLines, startHttpApp } from '../support/http-app';
import { createTestQueues, waitFor, wagerMessage } from '../support/sqs';

let baseUrl: string;
let close: () => Promise<void>;
let orm: MikroORM;
const logLines: string[] = [];
const touch = ledgerInvariantAfterEach(() => orm);

beforeAll(async () => {
  ({ baseUrl, close } = await startHttpApp({ logLines }));
  orm = await createTestOrm(2);
});
afterAll(async () => {
  await close();
  await orm.close(true);
});

/** Nenhum valor monetário nos logs: nem campos money/balance/amount, nem os valores usados. */
function assertNoFinancialPayload(lines: Record<string, any>[], amounts: string[]) {
  const raw = JSON.stringify(lines);
  expect(raw).not.toContain('"amount"');
  expect(raw).not.toContain('"balance":{');
  for (const a of amounts) expect(raw).not.toContain(a);
}

describe('logs HTTP', () => {
  test('linha por requisição com correlationId, walletId, providerId, transactionId; sem valores', async () => {
    logLines.length = 0;
    const w = (await http(baseUrl, 'POST', '/wallets', { body: { playerId: crypto.randomUUID(), initialBalance: { amount: '4321.98', currency: 'BRL' } } })).body;
    touch(w.id);
    const ext = `obs-${crypto.randomUUID()}`;
    const res = await http(baseUrl, 'POST', '/wagering/transactions', {
      headers: { 'idempotency-key': `provider-obs:${ext}`, 'x-correlation-id': 'corr-obs-1' },
      body: { providerId: 'provider-obs', externalTransactionId: ext, playerId: w.playerId, walletId: w.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '123.45', currency: 'BRL' } },
    });
    expect(res.status).toBe(200);

    const lines = parseLogLines(logLines);
    const request = lines.find((l) => l.msg === 'http request' && l.path === '/wagering/transactions');
    expect(request).toMatchObject({
      level: 'info',
      method: 'POST',
      status: 200,
      correlationId: 'corr-obs-1',
      walletId: w.id,
      providerId: 'provider-obs',
      transactionId: res.body.transactionId,
      service: 'wagering-processor',
    });
    expect(typeof request!.durationMs).toBe('number');
    assertNoFinancialPayload(lines, ['123.45', '4321.98', '4198.53']);
  });

  test('erros 4xx são logados como warn, com o contexto', async () => {
    logLines.length = 0;
    await http(baseUrl, 'GET', `/wallets/${crypto.randomUUID()}`, { headers: { 'x-correlation-id': 'corr-404' } });
    const line = parseLogLines(logLines).find((l) => l.msg === 'http request');
    expect(line).toMatchObject({ level: 'warn', status: 404, correlationId: 'corr-404' });
  });
});

describe('logs do consumer SQS', () => {
  test('mensagem processada: messageId, correlationId, walletId, providerId, transactionId; sem valores', async () => {
    const lines: string[] = [];
    const logger = createLogger({}, { write: (l: string) => void lines.push(l) });
    logger.level = 'info';
    const app = await createAppContext({ poolMax: 3 });
    const { gateway } = await createTestQueues('obs');
    const w = await openTestWallet(app, '777.77');
    touch(w.id);
    const cmd = wagerCommand(w, { money: { amount: '55.55', currency: 'BRL' }, providerId: 'provider-sqs' });
    const msg = wagerMessage(messageData(cmd), `msg-obs-${crypto.randomUUID()}`, { correlationId: 'corr-sqs-1' });
    await gateway.sendToInput(msg.body, w.id, crypto.randomUUID());
    const consumer = new WagerTransactionConsumer(gateway, app.processTransaction, app.metrics, logger, {
      consumerName: 'obs',
      maxAttempts: 3,
      waitTimeSeconds: 1,
      visibilityTimeoutSeconds: 60,
      batchSize: 10,
      shutdownGraceMs: 5_000,
    });
    consumer.start();
    const handled = await waitFor(async () => parseLogLines(lines).find((l) => l.msg === 'message handled'));
    await consumer.stop();
    await app.close();

    expect(handled).toMatchObject({
      messageId: msg.messageId,
      correlationId: 'corr-sqs-1',
      walletId: w.id,
      providerId: 'provider-sqs',
      disposition: 'processed',
      status: 'PROCESSED',
    });
    expect(handled.transactionId).toMatch(/^[0-9a-f-]{36}$/);
    assertNoFinancialPayload(parseLogLines(lines), ['55.55', '777.77', '722.22']);
  });
});

describe('health e métricas', () => {
  test('/health/ready verifica Postgres e SQS', async () => {
    const res = await http(baseUrl, 'GET', '/health/ready');
    expect(res.body).toEqual({ status: 'ok', checks: { postgres: 'up', sqs: 'up' } });
  });

  test('/health/ready em 503 diz QUAL dependência caiu (SQS real indisponível: fila apagada)', async () => {
    const { client, names, gateway } = await createTestQueues('ready');
    const other = await startHttpApp({ SQS_INPUT_QUEUE: names.input, SQS_DLQ: names.deadLetter, SQS_EVENTS_QUEUE: names.events });
    try {
      expect((await http(other.baseUrl, 'GET', '/health/ready')).status).toBe(200);
      await client.send(new DeleteQueueCommand({ QueueUrl: gateway.urls.input }));
      const down = await http(other.baseUrl, 'GET', '/health/ready');
      expect(down.status).toBe(503);
      expect(down.headers.get('retry-after')).toBe('1');
      expect(down.body).toEqual({ status: 'unavailable', checks: { postgres: 'up', sqs: 'down' } });
      expect((await http(other.baseUrl, 'GET', '/health/live')).status).toBe(200); // liveness não depende do SQS
    } finally {
      await other.close();
    }
  });

  test('latência medida em TODOS os desfechos HTTP (sucesso, payload inválido, conflito, negócio)', async () => {
    const countOf = async (outcome: string) => {
      const text = (await http<string>(baseUrl, 'GET', '/metrics')).body;
      const line = text.split('\n').find((l) => l.startsWith(`wagering_processing_duration_seconds_count{source="http",outcome="${outcome}"}`));
      return Number(line?.split(' ').at(-1) ?? 0);
    };
    const before = Object.fromEntries(await Promise.all(['PROCESSED', 'validation', 'conflict', 'business'].map(async (o) => [o, await countOf(o)])));
    const w = (await http(baseUrl, 'POST', '/wallets', { body: { playerId: crypto.randomUUID(), initialBalance: { amount: '10.00', currency: 'BRL' } } })).body;
    touch(w.id);
    const ext = `lat-${crypto.randomUUID()}`;
    const body = { providerId: 'provider-lat', externalTransactionId: ext, playerId: w.playerId, walletId: w.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '1.00', currency: 'BRL' } };
    const headers = { 'idempotency-key': `provider-lat:${ext}` };
    expect((await http(baseUrl, 'POST', '/wagering/transactions', { body, headers })).status).toBe(200);
    expect((await http(baseUrl, 'POST', '/wagering/transactions', { body: { ...body, money: { amount: '1.5', currency: 'BRL' } }, headers })).status).toBe(400);
    expect((await http(baseUrl, 'POST', '/wagering/transactions', { body: { ...body, money: { amount: '2.00', currency: 'BRL' } }, headers })).status).toBe(409);
    const ghost = { ...body, externalTransactionId: `lat-${crypto.randomUUID()}`, walletId: crypto.randomUUID() };
    expect((await http(baseUrl, 'POST', '/wagering/transactions', { body: ghost, headers: { 'idempotency-key': `provider-lat:${ghost.externalTransactionId}` } })).status).toBe(422);
    for (const outcome of ['PROCESSED', 'validation', 'conflict', 'business']) {
      expect(await countOf(outcome)).toBe(before[outcome] + 1);
    }
  });

  test('/metrics expõe as métricas exigidas', async () => {
    const text = (await http<string>(baseUrl, 'GET', '/metrics')).body;
    for (const name of [
      'wagering_transactions_total',
      'wagering_duplicates_detected_total',
      'wagering_db_lock_conflicts_total',
      'wagering_sqs_retries_total',
      'wagering_dlq_messages_total',
      'wagering_outbox_lag_seconds',
      'wagering_processing_duration_seconds',
      'wagering_reconciliation_mismatches_total',
    ]) {
      expect(text).toContain(`# TYPE ${name}`);
    }
  });
});
