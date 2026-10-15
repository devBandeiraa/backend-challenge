/**
 * Cenário do §11 com PROCESSOS REAIS:
 *   1. o Postgres confirma o commit (transação + eventos na outbox);
 *   2. o processo que reivindicou os eventos morre (SIGKILL) antes de publicar;
 *   3. OUTRO processo assume o trabalho;
 *   4. o evento é publicado;
 *   5. uma publicação duplicada (morte depois do SendMessage e antes do commit) continua segura.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { Subprocess } from 'bun';
import type { SQSClient } from '@aws-sdk/client-sqs';
import { createLogger } from '../../src/infrastructure/observability/logger';
import { AppMetrics } from '../../src/infrastructure/observability/metrics';
import { OutboxPublisher } from '../../src/infrastructure/workers/outbox-publisher';
import { type AppContext, countRows, createAppContext, openTestWallet, wagerCommand } from '../support/app-context';
import { ledgerInvariantAfterEach } from '../support/invariants';
import { createTestQueues, drain, type SqsGateway } from '../support/sqs';

let app: AppContext;
let gateway: SqsGateway;
let client: SQSClient;
let names: { input: string; deadLetter: string; events: string };
const touch = ledgerInvariantAfterEach(() => app.orm);
const children: Subprocess<'ignore', 'pipe', 'pipe'>[] = [];

beforeAll(async () => {
  app = await createAppContext({ poolMax: 4 });
  ({ gateway, client, names } = await createTestQueues('outbox-crash'));
  // Escoa o backlog de outros testes: aqui só os eventos deste arquivo ficam pendentes.
  const logger = createLogger({ test: 'outbox-crash' });
  logger.level = 'silent';
  const drainer = new OutboxPublisher(app.orm, gateway, new AppMetrics(), logger, { batchSize: 500 });
  while ((await drainer.publishOnce()).published > 0);
  await drain(client, gateway.urls.events, 500);
}, 180_000);
afterAll(async () => {
  for (const c of children) c.kill(9);
  await app.close();
});

function spawnPublisher(mode: 'crash-before-send' | 'crash-after-send' | 'publish', env: Record<string, string> = {}) {
  const child = Bun.spawn(['bun', 'run', 'test/support/outbox-publisher-process.ts'], {
    env: { ...process.env, MODE: mode, SQS_INPUT_QUEUE: names.input, SQS_DLQ: names.deadLetter, SQS_EVENTS_QUEUE: names.events, ...env },
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
  });
  children.push(child);
  return child;
}

/** Lê o stdout do processo até a linha JSON com o evento pedido. */
async function waitForEvent(child: Subprocess<'ignore', 'pipe', 'pipe'>, event: string, timeoutMs = 30_000): Promise<{ ids: string[] }> {
  const reader = child.stdout.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  const deadline = Date.now() + timeoutMs;
  try {
    while (Date.now() < deadline) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value);
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? ''; // última linha pode estar incompleta
      for (const line of lines) {
        if (!line.startsWith('{')) continue;
        const parsed = JSON.parse(line) as { event: string; ids: string[] };
        if (parsed.event === event) return parsed;
      }
    }
  } finally {
    reader.releaseLock();
  }
  throw new Error(`child never announced "${event}"; stderr: ${await new Response(child.stderr).text()}`);
}

/** Uma wallet com BET confirmada: 4 eventos na outbox (OPENING ×2 + BET ×2). */
async function committedEvents(): Promise<string[]> {
  const w = await openTestWallet(app, '100.00');
  touch(w.id);
  await app.processTransaction.execute(wagerCommand(w, { money: { amount: '10.00', currency: 'BRL' } }));
  const rows = await app.orm.em
    .fork()
    .execute<{ id: string }[]>(
      `SELECT id FROM outbox_messages WHERE aggregate_id = ? OR aggregate_id IN (SELECT id FROM wager_transactions WHERE wallet_id = ?)`,
      [w.id, w.id],
    );
  return rows.map((r) => r.id);
}

const unpublished = (ids: string[]) => countRows(app.orm, `outbox_messages WHERE id = ANY(?::uuid[]) AND published_at IS NULL`, [`{${ids.join(',')}}`]);

async function receivedEventIds(ids: string[]): Promise<string[]> {
  const messages = await drain(client, gateway.urls.events, 1_500);
  return messages.map((m) => (JSON.parse(m.body) as { eventId: string }).eventId).filter((id) => ids.includes(id));
}

describe('outbox com processos reais', () => {
  test('morre ANTES de publicar (segurando as linhas): OUTRO processo publica; cada evento chega uma vez', async () => {
    const ids = await committedEvents();
    expect(ids).toHaveLength(4);

    const a = spawnPublisher('crash-before-send');
    const claimed = await waitForEvent(a, 'claimed');
    expect(claimed.ids).toEqual(expect.arrayContaining(ids)); // A reivindicou (e travou) os nossos eventos
    a.kill(9);
    expect(await a.exited).not.toBe(0);
    expect(await unpublished(ids)).toBe(4); // nada foi marcado: o commit de A nunca aconteceu

    const b = spawnPublisher('publish', { EXPECT_IDS: ids.join(',') });
    await waitForEvent(b, 'published');
    expect(await b.exited).toBe(0);

    expect(await unpublished(ids)).toBe(0);
    expect((await receivedEventIds(ids)).sort()).toEqual([...ids].sort()); // nenhum perdido, nenhum duplicado
  }, 90_000);

  test('morre DEPOIS do SendMessage e antes do commit: OUTRO processo republica; a duplicata é absorvida', async () => {
    const ids = await committedEvents();
    const a = spawnPublisher('crash-after-send');
    const sent = await waitForEvent(a, 'sent');
    expect(sent.ids).toEqual(expect.arrayContaining(ids)); // 1º envio aconteceu
    a.kill(9);
    await a.exited;
    expect(await unpublished(ids)).toBe(4); // published_at não foi confirmado

    const b = spawnPublisher('publish', { EXPECT_IDS: ids.join(',') });
    await waitForEvent(b, 'published'); // 2º envio dos MESMOS eventos
    expect(await b.exited).toBe(0);

    // MessageDeduplicationId = eventId: a FIFO entrega uma vez; o consumidor deduplica pelo eventId.
    const received = await receivedEventIds(ids);
    expect(received.sort()).toEqual([...ids].sort());
    expect(new Set(received).size).toBe(received.length);
  }, 90_000);
});
