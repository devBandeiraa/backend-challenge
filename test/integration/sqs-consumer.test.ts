/**
 * Consumer SQS contra LocalStack e Postgres REAIS: inbox/redelivery, envelope tolerante + data
 * estrito, erros de negócio (ack), permanentes (DLQ), transitórios (retry com backoff) e, após
 * o limite, FAILED gravado ANTES da DLQ. Shutdown cooperativo.
 */
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { ExponentialBackoff } from '../../src/domain/messaging/backoff-policy';
import { AppMetrics } from '../../src/infrastructure/observability/metrics';
import { createLogger } from '../../src/infrastructure/observability/logger';
import { type ConsumerOptions, WagerTransactionConsumer } from '../../src/interfaces/sqs/wager-transaction.consumer';
import { type AppContext, countRows, createAppContext, messageData, openTestWallet, wagerCommand, walletBalance } from '../support/app-context';
import { ledgerInvariantAfterEach } from '../support/invariants';
import { createTestQueues, drain, type SqsGateway, waitFor, wagerMessage } from '../support/sqs';
import type { SQSClient } from '@aws-sdk/client-sqs';

let app: AppContext;
let gateway: SqsGateway;
let client: SQSClient;
const logger = createLogger({ test: 'sqs-consumer' });
logger.level = 'silent';
const touch = ledgerInvariantAfterEach(() => app.orm);
const running: WagerTransactionConsumer[] = [];

beforeAll(async () => {
  app = await createAppContext({ uow: { maxAttempts: 1, lockTimeoutMs: 200 } });
});
// Filas novas por teste: nenhuma mensagem de um teste vaza para outro.
beforeEach(async () => {
  ({ gateway, client } = await createTestQueues('consumer'));
});
afterEach(async () => {
  await Promise.all(running.splice(0).map((c) => c.stop()));
});
afterAll(async () => {
  await app.close();
});

function consumer(overrides: Partial<ConsumerOptions> = {}) {
  const c = new WagerTransactionConsumer(gateway, app.processTransaction, app.metrics, logger, {
    consumerName: 'test-consumer',
    maxAttempts: 3,
    waitTimeSeconds: 1,
    visibilityTimeoutSeconds: 30,
    batchSize: 10,
    shutdownGraceMs: 10_000,
    retryBackoff: new ExponentialBackoff({ baseMs: 1_000, factor: 1, maxMs: 1_000, jitterRatio: 0 }),
    ...overrides,
  });
  running.push(c);
  return c;
}

async function wallet(amount = '100.00') {
  const w = await openTestWallet(app, amount);
  touch(w.id);
  return w;
}

async function send(body: string, groupId: string) {
  await gateway.sendToInput(body, groupId, crypto.randomUUID());
}

async function duplicatesByLayer() {
  const values = (await app.metrics.duplicates.get()).values;
  const of = (layer: string) => values.find((v) => v.labels.layer === layer)?.value ?? 0;
  return { inbox: of('inbox'), idempotency: of('idempotency') };
}

async function statusOf(idempotencyKey: string) {
  const [row] = await app.orm.em
    .fork()
    .execute<{ status: string; failure_code: string | null }[]>(
      `SELECT status, failure_code FROM wager_transactions WHERE idempotency_key = ?`,
      [idempotencyKey],
    );
  return row;
}

describe('processamento', () => {
  test('mensagem do enunciado é processada pelo MESMO use case, gravada na inbox e removida da fila (ack após commit)', async () => {
    const w = await wallet('100.00');
    const cmd = wagerCommand(w, { money: { amount: '25.00', currency: 'BRL' } });
    const msg = wagerMessage(messageData(cmd));
    await send(msg.body, w.id);
    consumer().start();

    await waitFor(async () => (await statusOf(cmd.idempotencyKey))?.status === 'PROCESSED');
    expect(await walletBalance(app.orm, w.id)).toBe('75.00');
    expect(await countRows(app.orm, `inbox_messages WHERE message_id = ? AND processed_at IS NOT NULL`, [msg.messageId])).toBe(1);
    await waitFor(async () => (await drain(client, gateway.urls.input, 300)).length === 0 || undefined);
  });

  test('envelope tolerante: campos extras e sem type/occurredAt; sem messageId usa o id do SQS', async () => {
    const w = await wallet();
    const cmd = wagerCommand(w);
    await send(JSON.stringify({ data: messageData(cmd), traceparent: '00-abc', schemaVersion: 7 }), w.id);
    consumer().start();
    await waitFor(async () => (await statusOf(cmd.idempotencyKey))?.status === 'PROCESSED');
  });

  test('UUID em maiúsculas na fila é normalizado: a BET é processada', async () => {
    const w = await wallet('100.00');
    const cmd = wagerCommand({ id: w.id.toUpperCase(), playerId: w.playerId.toUpperCase() });
    const c = consumer();
    await send(wagerMessage(messageData(cmd)).body, w.id);
    c.start();
    await waitFor(async () => (await statusOf(cmd.idempotencyKey))?.status === 'PROCESSED' || undefined, 15_000);
    expect(await walletBalance(app.orm, w.id)).toBe('90.00');
  });

  test('redelivery da MESMA mensagem (mesmo messageId): inbox detecta, nenhum efeito novo', async () => {
    const w = await wallet('100.00');
    const cmd = wagerCommand(w, { money: { amount: '10.00', currency: 'BRL' } });
    const msg = wagerMessage(messageData(cmd));
    const dupBefore = await duplicatesByLayer();
    await send(msg.body, w.id);
    await send(msg.body, w.id); // produtor reenviou (outro dedup id no SQS, mesmo messageId lógico)
    const c = consumer();
    c.start();
    await waitFor(async () => (await countRows(app.orm, `inbox_messages WHERE message_id = ?`, [msg.messageId])) === 1 || undefined);
    await Bun.sleep(1_500);
    expect(await walletBalance(app.orm, w.id)).toBe('90.00');
    expect(await countRows(app.orm, `wallet_ledger_entries l JOIN wager_transactions t ON t.id = l.transaction_id WHERE t.idempotency_key = ?`, [cmd.idempotencyKey])).toBe(1);
    // uma redelivery = UMA duplicata (inbox), não duas (inbox + replay da key)
    const dupAfter = await duplicatesByLayer();
    expect(dupAfter.inbox - dupBefore.inbox).toBe(1);
    expect(dupAfter.idempotency - dupBefore.idempotency).toBe(0);
  });

  test('mesma operação com OUTRO messageId: inbox não pega, idempotency key pega (replay)', async () => {
    const w = await wallet('100.00');
    const cmd = wagerCommand(w, { money: { amount: '10.00', currency: 'BRL' } });
    await send(wagerMessage(messageData(cmd)).body, w.id);
    await send(wagerMessage(messageData(cmd)).body, w.id);
    consumer().start();
    await waitFor(async () => (await countRows(app.orm, `inbox_messages i JOIN wager_transactions t ON t.id = i.transaction_id WHERE t.idempotency_key = ?`, [cmd.idempotencyKey])) === 2 || undefined);
    expect(await walletBalance(app.orm, w.id)).toBe('90.00');
  });

  test('rejeição de negócio (sem saldo) e WALLET_NOT_FOUND: ack, sem DLQ', async () => {
    const w = await wallet('5.00');
    const cmd = wagerCommand(w, { money: { amount: '10.00', currency: 'BRL' } });
    const ghost = wagerCommand({ id: crypto.randomUUID(), playerId: crypto.randomUUID() });
    await send(wagerMessage(messageData(cmd)).body, w.id);
    await send(wagerMessage(messageData(ghost)).body, ghost.walletId);
    consumer().start();
    await waitFor(async () => (await statusOf(cmd.idempotencyKey))?.failure_code === 'INSUFFICIENT_FUNDS');
    await Bun.sleep(1_500);
    expect(await drain(client, gateway.urls.deadLetter, 500)).toHaveLength(0);
    expect(await drain(client, gateway.urls.input, 300)).toHaveLength(0);
  });

  test('crédito que estouraria NUMERIC(19,2): REJECTED BALANCE_LIMIT_EXCEEDED, ack na 1ª entrega, sem FAILED nem DLQ', async () => {
    const w = await wallet('1000.00');
    const cmd = wagerCommand(w, { kind: 'WIN', money: { amount: '99999999999999999.99', currency: 'BRL' } });
    const retriesBefore = (await app.metrics.sqsRetries.get()).values[0]?.value ?? 0;
    await send(wagerMessage(messageData(cmd)).body, w.id);
    consumer().start();
    await waitFor(async () => (await statusOf(cmd.idempotencyKey))?.failure_code === 'BALANCE_LIMIT_EXCEEDED');
    await Bun.sleep(1_500);
    expect(await statusOf(cmd.idempotencyKey)).toMatchObject({ status: 'REJECTED' });
    expect(await walletBalance(app.orm, w.id)).toBe('1000.00');
    expect(await drain(client, gateway.urls.deadLetter, 500)).toHaveLength(0);
    expect(await drain(client, gateway.urls.input, 300)).toHaveLength(0);
    expect((await app.metrics.sqsRetries.get()).values[0]?.value ?? 0).toBe(retriesBefore);
  });
});

describe('erros permanentes → DLQ', () => {
  test.each([
    ['JSON inválido', '{not json', 'invalid_json'],
    ['data com campo desconhecido (data é estrito)', JSON.stringify({ data: { ...messageData(wagerCommand({ id: crypto.randomUUID(), playerId: crypto.randomUUID() })), extra: 1 } }), 'invalid_data'],
    // a forma passa no zod (é string); o conteúdo é validado pelo Money do domínio
    ['data com valor "25.5" (escala fixa)', JSON.stringify({ data: { ...messageData(wagerCommand({ id: crypto.randomUUID(), playerId: crypto.randomUUID() })), money: { amount: '25.5', currency: 'BRL' } } }), 'INVALID_MONEY'],
    ['data sem idempotencyKey', JSON.stringify({ data: (({ idempotencyKey: _k, ...d }) => d)(messageData(wagerCommand({ id: crypto.randomUUID(), playerId: crypto.randomUUID() }))) }), 'invalid_data'],
  ])('%s', async (_label, body, reason) => {
    await send(body, 'g-' + crypto.randomUUID());
    consumer().start();
    const dlq = await waitFor(async () => {
      const got = await drain(client, gateway.urls.deadLetter, 500);
      return got.length > 0 ? got : undefined;
    });
    expect(dlq[0]!.attributes.failureReason).toBe(reason);
    expect(dlq[0]!.body).toBe(body); // corpo original preservado para inspeção/redrive
  });

  test('mesmo messageId com conteúdo DIFERENTE: INBOX_PAYLOAD_MISMATCH → DLQ', async () => {
    const w = await wallet();
    const first = wagerCommand(w);
    const msgId = `msg-${crypto.randomUUID()}`;
    await send(wagerMessage(messageData(first), msgId).body, w.id);
    await send(wagerMessage(messageData(wagerCommand(w)), msgId).body, w.id);
    consumer().start();
    const dlq = await waitFor(async () => {
      const got = await drain(client, gateway.urls.deadLetter, 500);
      return got.length > 0 ? got : undefined;
    });
    expect(dlq[0]!.attributes.failureReason).toBe('INBOX_PAYLOAD_MISMATCH');
  });
});

describe('providerId reservado pela fila', () => {
  test('providerId "internal" no data → DLQ invalid_data, nenhuma transação criada', async () => {
    const w = await wallet();
    const cmd = wagerCommand(w, { providerId: 'internal' });
    await send(wagerMessage(messageData(cmd)).body, w.id);
    consumer().start();
    const dlq = await waitFor(async () => {
      const got = await drain(client, gateway.urls.deadLetter, 500);
      return got.length > 0 ? got : undefined;
    });
    expect(dlq[0]!.attributes.failureReason).toBe('invalid_data');
    expect(await countRows(app.orm, `wager_transactions WHERE wallet_id = ? AND kind <> 'OPENING'`, [w.id])).toBe(0);
  });
});

describe('transitório → retry com backoff → limite → FAILED antes da DLQ', () => {
  test('wallet travada por outra transação: retries medidos, FAILED gravado, DLQ com motivo; replay devolve FAILED', async () => {
    const w = await wallet('100.00');
    const cmd = wagerCommand(w, { money: { amount: '10.00', currency: 'BRL' } });
    const msg = wagerMessage(messageData(cmd));

    // FOR NO KEY UPDATE bloqueia o lock da wallet no caminho de processamento (55P03 após 200ms),
    // mas NÃO bloqueia o INSERT do FAILED (que só precisa de KEY SHARE pela FK).
    let release!: () => void;
    const released = new Promise<void>((r) => (release = r));
    let locked!: () => void;
    const isLocked = new Promise<void>((r) => (locked = r));
    const holder = app.orm.em.fork().transactional(async (em) => {
      await em.execute(`SELECT id FROM wallets WHERE id = ? FOR NO KEY UPDATE`, [w.id]);
      locked();
      await released;
    });
    await isLocked;

    const retriesBefore = (await app.metrics.sqsRetries.get()).values[0]?.value ?? 0;
    await send(msg.body, w.id);
    consumer({ maxAttempts: 3 }).start();

    const dlq = await waitFor(async () => {
      const got = await drain(client, gateway.urls.deadLetter, 500);
      return got.length > 0 ? got : undefined;
    }, 30_000);
    release();
    await holder;

    expect(dlq[0]!.attributes.failureReason).toBe('retries_exhausted');
    expect(dlq[0]!.attributes.receiveCount).toBe('3');
    expect((await app.metrics.sqsRetries.get()).values[0]!.value - retriesBefore).toBe(2);
    expect(await statusOf(cmd.idempotencyKey)).toEqual({ status: 'FAILED', failure_code: 'PROCESSING_FAILED' });
    expect(await countRows(app.orm, `inbox_messages WHERE message_id = ? AND processed_at IS NOT NULL`, [msg.messageId])).toBe(1);
    expect(await walletBalance(app.orm, w.id)).toBe('100.00'); // nada aplicado

    // FAILED é terminal para a key: reenviar devolve o mesmo resultado (HTTP: 422 PROCESSING_FAILED)
    const replay = await app.processTransaction.execute(cmd);
    expect(replay).toMatchObject({ status: 'FAILED', failureCode: 'PROCESSING_FAILED', idempotentReplay: true });
  }, 40_000);
});

describe('erro NÃO classificado é tratado como transitório até o limite', () => {
  /** Use case real com falha de programação injetada nas N primeiras chamadas (banco e fila reais). */
  function flaky(failures: number) {
    let calls = 0;
    return {
      executeFromMessage: (...args: Parameters<typeof app.processTransaction.executeFromMessage>) => {
        calls += 1;
        if (calls <= failures) throw new TypeError(`unexpected shape (call ${calls})`);
        return app.processTransaction.executeFromMessage(...args);
      },
      recordFailure: (...args: Parameters<typeof app.processTransaction.recordFailure>) => app.processTransaction.recordFailure(...args),
    } as unknown as typeof app.processTransaction;
  }

  test('falha inesperada na 1ª tentativa NÃO gera FAILED: retry e depois PROCESSED', async () => {
    const w = await wallet('100.00');
    const cmd = wagerCommand(w, { money: { amount: '10.00', currency: 'BRL' } });
    await send(wagerMessage(messageData(cmd)).body, w.id);
    const c = new WagerTransactionConsumer(gateway, flaky(1), app.metrics, logger, {
      consumerName: 'test-consumer',
      maxAttempts: 3,
      waitTimeSeconds: 1,
      visibilityTimeoutSeconds: 30,
      batchSize: 10,
      shutdownGraceMs: 5_000,
      retryBackoff: new ExponentialBackoff({ baseMs: 1_000, factor: 1, maxMs: 1_000, jitterRatio: 0 }),
    });
    running.push(c);
    c.start();
    await waitFor(async () => (await statusOf(cmd.idempotencyKey))?.status === 'PROCESSED', 20_000);
    expect(await drain(client, gateway.urls.deadLetter, 500)).toHaveLength(0);
    expect(await walletBalance(app.orm, w.id)).toBe('90.00');
  });

  test('falha inesperada persistente: retries até o limite, só então FAILED + DLQ (unexpected_error)', async () => {
    const w = await wallet('100.00');
    const cmd = wagerCommand(w);
    await send(wagerMessage(messageData(cmd)).body, w.id);
    const c = new WagerTransactionConsumer(gateway, flaky(Number.MAX_SAFE_INTEGER), app.metrics, logger, {
      consumerName: 'test-consumer',
      maxAttempts: 3,
      waitTimeSeconds: 1,
      visibilityTimeoutSeconds: 30,
      batchSize: 10,
      shutdownGraceMs: 5_000,
      retryBackoff: new ExponentialBackoff({ baseMs: 1_000, factor: 1, maxMs: 1_000, jitterRatio: 0 }),
    });
    running.push(c);
    c.start();
    const dlq = await waitFor(async () => {
      const got = await drain(client, gateway.urls.deadLetter, 500);
      return got.length > 0 ? got : undefined;
    }, 30_000);
    expect(dlq[0]!.attributes).toMatchObject({ failureReason: 'unexpected_error', receiveCount: '3' });
    expect(await statusOf(cmd.idempotencyKey)).toEqual({ status: 'FAILED', failure_code: 'PROCESSING_FAILED' });
  });
});

describe('heartbeat de visibilidade', () => {
  test('processamento mais longo que o visibility timeout: outro consumer NÃO recebe a mensagem', async () => {
    const w = await wallet('100.00');
    const cmd = wagerCommand(w, { money: { amount: '10.00', currency: 'BRL' } });
    const slowApp = await createAppContext({ poolMax: 4, uow: { maxAttempts: 1, lockTimeoutMs: 15_000 } });
    let release!: () => void;
    const released = new Promise<void>((r) => (release = r));
    let locked!: () => void;
    const isLocked = new Promise<void>((r) => (locked = r));
    const holder = app.orm.em.fork().transactional(async (em) => {
      await em.execute(`SELECT id FROM wallets WHERE id = ? FOR NO KEY UPDATE`, [w.id]);
      locked();
      await released;
    });
    await isLocked;

    await send(wagerMessage(messageData(cmd)).body, w.id);
    const options = {
      consumerName: 'test-consumer',
      maxAttempts: 3,
      waitTimeSeconds: 1,
      visibilityTimeoutSeconds: 2, // processamento vai durar ~6s: 3× o timeout
      batchSize: 10,
      shutdownGraceMs: 15_000,
    };
    const metricsA = new AppMetrics();
    const metricsB = new AppMetrics();
    const a = new WagerTransactionConsumer(gateway, slowApp.processTransaction, metricsA, logger, options);
    a.start();
    await Bun.sleep(1_500); // A recebeu e está esperando o lock
    const b = new WagerTransactionConsumer(gateway, app.processTransaction, metricsB, logger, options);
    b.start();
    await Bun.sleep(4_500); // > 2× visibility timeout: sem heartbeat, B já teria recebido
    release();
    await holder;
    await waitFor(async () => (await statusOf(cmd.idempotencyKey))?.status === 'PROCESSED', 10_000);
    await Promise.all([a.stop(), b.stop()]);

    const handledByB = (await metricsB.sqsMessages.get()).values.reduce((s, v) => s + v.value, 0);
    expect(handledByB).toBe(0);
    expect((await metricsA.sqsMessages.get()).values.find((v) => v.labels.outcome === 'processed')?.value).toBe(1);
    expect(await walletBalance(app.orm, w.id)).toBe('90.00');
    await slowApp.close();
  });
});

describe('shutdown (SIGTERM)', () => {
  test('stop() conclui a mensagem em andamento e devolve à fila as que não começaram', async () => {
    const w = await wallet('100.00');
    const inFlight = wagerCommand(w, { money: { amount: '1.00', currency: 'BRL' } });
    const waiting = wagerCommand(w, { money: { amount: '2.00', currency: 'BRL' } });

    // Segura a wallet para a 1ª mensagem ficar "em andamento" (lock_timeout maior que a espera).
    const slowApp = await createAppContext({ poolMax: 5, uow: { maxAttempts: 1, lockTimeoutMs: 5_000 } });
    let release!: () => void;
    const released = new Promise<void>((r) => (release = r));
    let locked!: () => void;
    const isLocked = new Promise<void>((r) => (locked = r));
    const holder = app.orm.em.fork().transactional(async (em) => {
      await em.execute(`SELECT id FROM wallets WHERE id = ? FOR NO KEY UPDATE`, [w.id]);
      locked();
      await released;
    });
    await isLocked;

    await send(wagerMessage(messageData(inFlight)).body, w.id);
    await send(wagerMessage(messageData(waiting)).body, w.id);
    const c = new WagerTransactionConsumer(gateway, slowApp.processTransaction, slowApp.metrics, logger, {
      consumerName: 'test-consumer',
      maxAttempts: 3,
      waitTimeSeconds: 1,
      visibilityTimeoutSeconds: 30, // sem o release explícito, a 2ª só voltaria após 30s
      batchSize: 10,
      shutdownGraceMs: 10_000,
    });
    c.start();
    // a 1ª mensagem já inseriu a transação (PENDING, não confirmada) e espera o lock da wallet
    await waitFor(async () => {
      const [row] = await app.orm.em.fork().execute<{ n: string }[]>(
        `SELECT count(*)::text AS n FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND datname = current_database()`,
      );
      return Number(row?.n) > 0 || undefined;
    }, 10_000);

    const stopping = c.stop(); // "SIGTERM"
    await Bun.sleep(300);
    release(); // o trabalho em andamento consegue terminar
    await holder;
    await stopping;

    expect((await statusOf(inFlight.idempotencyKey))?.status).toBe('PROCESSED');
    expect(await statusOf(waiting.idempotencyKey)).toBeUndefined(); // não começou

    // A não iniciada voltou para a fila na hora (visibility 0): outro consumer pega em segundos.
    const startedAt = Date.now();
    consumer().start();
    await waitFor(async () => (await statusOf(waiting.idempotencyKey))?.status === 'PROCESSED', 10_000);
    expect(Date.now() - startedAt).toBeLessThan(10_000);
    expect(await walletBalance(app.orm, w.id)).toBe('97.00');
    await slowApp.close();
  }, 30_000);
});

describe('SIGTERM durante um long poll', () => {
  test('o poll em andamento NÃO é abortado: o que ele recebe é devolvido na hora e outro consumer processa em segundos', async () => {
    const wallets = await Promise.all([1, 2, 3].map(() => wallet('100.00')));
    const cmds = wallets.map((w) => wagerCommand(w, { money: { amount: '5.00', currency: 'BRL' } }));
    const options = {
      consumerName: 'test-consumer',
      maxAttempts: 3,
      waitTimeSeconds: 5, // long poll longo: o SIGTERM chega no meio dele
      visibilityTimeoutSeconds: 30, // mensagem presa só voltaria depois disto
      batchSize: 10,
      shutdownGraceMs: 15_000,
    };
    const metricsA = new AppMetrics();
    const a = new WagerTransactionConsumer(gateway, app.processTransaction, metricsA, logger, options);
    a.start();
    await Bun.sleep(500); // A está bloqueado no ReceiveMessage (fila vazia), sozinho

    const stopStartedAt = Date.now();
    const stopping = a.stop(); // "SIGTERM" com o long poll em andamento
    for (const c of cmds) await send(wagerMessage(messageData(c)).body, c.walletId); // chegam ao poll de A
    await stopping;
    const stopMs = Date.now() - stopStartedAt;

    // o poll de A (não abortado) recebeu mensagens e as devolveu (visibility 0) sem processar nenhuma.
    // O SQS encerra o long poll assim que há mensagem: o poll pode trazer só a 1ª; as outras nem passam por A.
    const outcomesA = Object.fromEntries((await metricsA.sqsMessages.get()).values.map((v) => [v.labels.outcome, v.value]));
    expect(Object.keys(outcomesA)).toEqual(['released']);
    expect(outcomesA.released).toBeGreaterThanOrEqual(1);
    expect(stopMs).toBeLessThan(options.waitTimeSeconds * 1_000 + 2_000); // terminou com o poll, sem abortá-lo
    for (const c of cmds) expect(await statusOf(c.idempotencyKey)).toBeUndefined();

    // outro consumer ("outra réplica") processa em segundos, não depois do visibility timeout
    const handoffStartedAt = Date.now();
    consumer({ waitTimeSeconds: 1 }).start();
    await waitFor(async () => {
      const statuses = await Promise.all(cmds.map((c) => statusOf(c.idempotencyKey)));
      return statuses.every((r) => r?.status === 'PROCESSED') || undefined;
    }, 10_000);
    const handoffMs = Date.now() - handoffStartedAt;
    expect(handoffMs).toBeLessThan(5_000); // << 30 s de visibility
    for (const w of wallets) expect(await walletBalance(app.orm, w.id)).toBe('95.00');
  }, 40_000);
});

describe('teto do heartbeat (maxProcessingMs)', () => {
  test('entrega travada além do teto: a visibilidade deixa de ser renovada, outro consumer recebe e a inbox evita o 2º efeito', async () => {
    const w = await wallet('100.00');
    const cmd = wagerCommand(w, { money: { amount: '10.00', currency: 'BRL' } });
    // lock_timeout longo: as duas entregas esperam (wallet e PK da inbox) em vez de desistir
    const slowApp = await createAppContext({ poolMax: 4, uow: { maxAttempts: 1, lockTimeoutMs: 20_000 } });
    let release!: () => void;
    const released = new Promise<void>((r) => (release = r));
    let locked!: () => void;
    const isLocked = new Promise<void>((r) => (locked = r));
    const holder = app.orm.em.fork().transactional(async (em) => {
      await em.execute(`SELECT id FROM wallets WHERE id = ? FOR NO KEY UPDATE`, [w.id]);
      locked();
      await released;
    });
    await isLocked;

    await send(wagerMessage(messageData(cmd)).body, w.id);
    const options = {
      consumerName: 'test-consumer',
      maxAttempts: 3,
      waitTimeSeconds: 1,
      visibilityTimeoutSeconds: 2,
      batchSize: 10,
      shutdownGraceMs: 20_000,
      maxProcessingMs: 3_000, // renova por ~3 s e para: a mensagem reaparece ~2 s depois
    };
    const metricsA = new AppMetrics();
    const metricsB = new AppMetrics();
    const a = new WagerTransactionConsumer(gateway, slowApp.processTransaction, metricsA, logger, options);
    a.start();
    await Bun.sleep(1_000);
    const b = new WagerTransactionConsumer(gateway, slowApp.processTransaction, metricsB, logger, options);
    b.start();
    // B só recebe depois do teto (sem teto, o teste anterior prova que nunca receberia)
    await waitFor(async () => (await countRows(app.orm, `pg_stat_activity WHERE wait_event_type = 'Lock' AND query ILIKE '%inbox_messages%'`, [])) >= 1 || undefined, 15_000, 200);
    release();
    await holder;
    await waitFor(async () => (await metricsB.sqsMessages.get()).values.some((v) => v.labels.outcome === 'duplicate') || undefined, 15_000);
    await Promise.all([a.stop(), b.stop()]);
    await slowApp.close();

    expect((await metricsA.sqsMessages.get()).values.find((v) => v.labels.outcome === 'processed')?.value).toBe(1);
    expect(await walletBalance(app.orm, w.id)).toBe('90.00');
    expect(await countRows(app.orm, `wallet_ledger_entries l JOIN wager_transactions t ON t.id = l.transaction_id WHERE t.idempotency_key = ?`, [cmd.idempotencyKey])).toBe(1);
  }, 45_000);
});

describe('ordem FIFO do grupo', () => {
  test('mensagem em retry segura as seguintes do MESMO grupo: BET1 (90) é aplicada antes da BET2 (20)', async () => {
    const w = await wallet('100.00');
    const bet1 = wagerCommand(w, { money: { amount: '90.00', currency: 'BRL' } });
    const bet2 = wagerCommand(w, { money: { amount: '20.00', currency: 'BRL' } });

    // outra transação segura a wallet: a BET1 estoura o lock_timeout (200 ms, 1 tentativa) e vai para retry
    let release!: () => void;
    const released = new Promise<void>((r) => (release = r));
    let locked!: () => void;
    const isLocked = new Promise<void>((r) => (locked = r));
    const holder = app.orm.em.fork().transactional(async (em) => {
      await em.execute(`SELECT id FROM wallets WHERE id = ? FOR NO KEY UPDATE`, [w.id]);
      locked();
      await released;
    });
    await isLocked;

    // as duas no MESMO grupo, enviadas antes de o consumer começar: chegam no mesmo lote do ReceiveMessage
    await send(wagerMessage(messageData(bet1)).body, w.id);
    await send(wagerMessage(messageData(bet2)).body, w.id);
    const retriesBefore = (await app.metrics.sqsRetries.get()).values[0]?.value ?? 0;
    const c = consumer();
    c.start();
    // solta o lock no instante em que a BET1 entra em retry: é a janela em que a BET2 passava na frente
    await waitFor(async () => ((await app.metrics.sqsRetries.get()).values[0]?.value ?? 0) > retriesBefore || undefined, 15_000, 5);
    release();
    await holder;

    await waitFor(async () => {
      const [a, b] = [await statusOf(bet1.idempotencyKey), await statusOf(bet2.idempotencyKey)];
      return (a && b && a.status !== 'PENDING' && b.status !== 'PENDING') || undefined;
    }, 20_000);
    expect(await statusOf(bet1.idempotencyKey)).toEqual({ status: 'PROCESSED', failure_code: null });
    expect(await statusOf(bet2.idempotencyKey)).toEqual({ status: 'REJECTED', failure_code: 'INSUFFICIENT_FUNDS' });
    expect(await walletBalance(app.orm, w.id)).toBe('10.00');
    const heldBehind = (await app.metrics.sqsMessages.get()).values.find((v) => v.labels.outcome === 'held_behind')?.value ?? 0;
    expect(heldBehind).toBeGreaterThanOrEqual(1); // a BET2 foi devolvida atrás da BET1, não processada fora de ordem
  }, 60_000);
});
