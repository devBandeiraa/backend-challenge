/**
 * ≥ 3 INSTÂNCIAS REAIS: o stack do docker compose (3 réplicas da app, cada uma com pool próprio,
 * consumer SQS, publisher da outbox e worker de referências) atrás do nginx.
 *   bun run test:multi      (sobe/reconstrói o stack e roda este arquivo)
 * Não roda no `bun test` padrão: depende do stack completo.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import { DEFAULT_QUEUE_NAMES, createSqsClient } from '../../src/infrastructure/messaging/sqs/sqs-config';
import { SqsGateway } from '../../src/infrastructure/messaging/sqs/sqs-gateway';
import { assertLedgerInvariant, barrier, countRows, walletBalance } from '../support/app-context';
import { createTestOrm } from '../support/db';
import { waitFor, wagerMessage } from '../support/sqs';

const BASE_URL = process.env.MULTI_INSTANCE_URL ?? 'http://localhost:8088';
let orm: MikroORM;
let gateway: SqsGateway;
const touched = new Set<string>();

beforeAll(async () => {
  const health = await fetch(`${BASE_URL}/health/ready`).catch(() => undefined);
  if (!health?.ok) throw new Error(`stack not ready at ${BASE_URL}: run "bun run test:multi" (docker compose up --build --wait)`);
  orm = await createTestOrm(5);
  gateway = await SqsGateway.connect(createSqsClient(), DEFAULT_QUEUE_NAMES);
});
afterAll(async () => {
  for (const id of touched) await assertLedgerInvariant(orm, id);
  await orm?.close(true);
});

async function post(path: string, body: unknown, headers: Record<string, string> = {}) {
  const res = await fetch(`${BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: JSON.stringify(body),
  });
  return { status: res.status, instance: res.headers.get('x-instance-id'), body: (await res.json()) as any };
}

async function createWallet(amount: string) {
  const res = await post('/wallets', { playerId: crypto.randomUUID(), initialBalance: { amount, currency: 'BRL' } });
  expect(res.status).toBe(201);
  touched.add(res.body.id);
  return res.body as { id: string; playerId: string };
}

function tx(w: { id: string; playerId: string }, overrides: Record<string, unknown> = {}) {
  return {
    providerId: 'provider-a',
    externalTransactionId: `mi-${crypto.randomUUID()}`,
    playerId: w.playerId,
    walletId: w.id,
    roundId: 'r1',
    gameId: 'g1',
    kind: 'BET',
    money: { amount: '10.00', currency: 'BRL' },
    ...overrides,
  };
}

const submit = (body: ReturnType<typeof tx>) =>
  post('/wagering/transactions', body, { 'idempotency-key': `${body.providerId}:${body.externalTransactionId}` });

/** Dispara N chamadas HTTP liberadas juntas por uma barreira. */
async function burst<T>(n: number, call: (i: number) => Promise<T>): Promise<T[]> {
  const ready = barrier(n);
  return Promise.all(Array.from({ length: n }, async (_, i) => (await ready(), call(i))));
}

describe('3 réplicas atrás do nginx', () => {
  test('requisições realmente distribuídas por ≥ 3 instâncias', async () => {
    const instances = new Set<string>();
    for (let i = 0; i < 30; i++) instances.add((await fetch(`${BASE_URL}/health/live`)).headers.get('x-instance-id')!);
    expect(instances.size).toBeGreaterThanOrEqual(3);
  });

  test('a mesma aposta 50x em paralelo através das réplicas: um único débito', async () => {
    const w = await createWallet('1000.00');
    const body = tx(w, { money: { amount: '25.00', currency: 'BRL' } });
    const results = await burst(50, () => submit(body));
    expect(results.every((r) => r.status === 200 && r.body.balance.amount === '975.00')).toBe(true);
    expect(new Set(results.map((r) => r.body.transactionId)).size).toBe(1);
    expect(results.filter((r) => !r.body.idempotentReplay)).toHaveLength(1);
    expect(new Set(results.map((r) => r.instance)).size).toBeGreaterThanOrEqual(3);
    expect(await countRows(orm, `wallet_ledger_entries WHERE wallet_id = ? AND direction = 'DEBIT'`, [w.id])).toBe(1);
  });

  test('saldo 100 + duas apostas de 80 simultâneas em réplicas diferentes (10 rodadas)', async () => {
    for (let round = 0; round < 10; round++) {
      const w = await createWallet('100.00');
      const results = await burst(2, () => submit(tx(w, { money: { amount: '80.00', currency: 'BRL' } })));
      expect(results.map((r) => r.status).sort()).toEqual([200, 422]);
      expect(results.find((r) => r.status === 422)!.body.failureCode).toBe('INSUFFICIENT_FUNDS');
      expect(await walletBalance(orm, w.id)).toBe('20.00');
      expect(await countRows(orm, `wallet_ledger_entries WHERE wallet_id = ? AND direction = 'DEBIT'`, [w.id])).toBe(1);
      await assertLedgerInvariant(orm, w.id);
    }
  });

  test('wallets distintas em paralelo pelas réplicas', async () => {
    const wallets = await Promise.all(Array.from({ length: 10 }, () => createWallet('100.00')));
    const results = await burst(100, (i) => submit(tx(wallets[i % 10]!, { money: { amount: '9.00', currency: 'BRL' } })));
    expect(results.every((r) => r.status === 200)).toBe(true);
    for (const w of wallets) expect(await walletBalance(orm, w.id)).toBe('10.00');
  });
});

describe('SQS consumido por 3 consumers concorrentes', () => {
  test('mensagens duplicadas e fora de ordem: cada efeito aplicado uma vez; eventos publicados', async () => {
    const wallets = await Promise.all(Array.from({ length: 8 }, () => createWallet('100.00')));
    const expected = new Map<string, string>();
    const keys: string[] = [];
    for (const w of wallets) {
      const bet = { ...tx(w, { money: { amount: '30.00', currency: 'BRL' } }), idempotencyKey: '' };
      bet.idempotencyKey = `${bet.providerId}:${bet.externalTransactionId}`;
      const refund = {
        ...tx(w, { kind: 'REFUND', money: { amount: '30.00', currency: 'BRL' }, referenceExternalTransactionId: bet.externalTransactionId }),
        idempotencyKey: '',
      };
      refund.idempotencyKey = `${refund.providerId}:${refund.externalTransactionId}`;
      const win = { ...tx(w, { kind: 'WIN', money: { amount: '5.00', currency: 'BRL' } }), idempotencyKey: '' };
      win.idempotencyKey = `${win.providerId}:${win.externalTransactionId}`;
      keys.push(bet.idempotencyKey, refund.idempotencyKey, win.idempotencyKey);

      // REFUND antes da BET; WIN enviado 3x (mesmo messageId = redelivery; novo messageId = reenvio do provedor)
      const winMsg = wagerMessage(win);
      for (const m of [wagerMessage(refund), wagerMessage(bet), winMsg, winMsg, wagerMessage(win)]) {
        await gateway.sendToInput(m.body, w.id, crypto.randomUUID());
      }
      expected.set(w.id, '105.00'); // 100 − 30 + 30 + 5
    }

    await waitFor(async () => {
      const [row] = await orm.em
        .fork()
        .execute<{ n: string }[]>(
          `SELECT count(*)::text AS n FROM wager_transactions WHERE idempotency_key = ANY(?::text[]) AND status = 'PROCESSED'`,
          [`{${keys.map((k) => `"${k}"`).join(',')}}`],
        );
      return Number(row?.n) === keys.length || undefined;
    }, 60_000, 500);

    for (const [walletId, balance] of expected) {
      expect(await walletBalance(orm, walletId)).toBe(balance);
      expect(await countRows(orm, `wallet_ledger_entries WHERE wallet_id = ?`, [walletId])).toBe(4); // OPENING, BET, REFUND, WIN
      await assertLedgerInvariant(orm, walletId);
    }
    // 3 publishers concorrentes escoam a outbox dessas wallets
    await waitFor(async () => {
      const pending = await countRows(
        orm,
        `outbox_messages o WHERE o.published_at IS NULL AND (o.aggregate_id = ANY(?::uuid[]) OR o.aggregate_id IN (SELECT id FROM wager_transactions WHERE wallet_id = ANY(?::uuid[])))`,
        [`{${[...expected.keys()].join(',')}}`, `{${[...expected.keys()].join(',')}}`],
      );
      return pending === 0 || undefined;
    }, 30_000, 500);
  }, 120_000);

  test('SIGTERM numa réplica durante o consumo: nada perdido, nada duplicado, réplica volta', async () => {
    const wallets = await Promise.all(Array.from({ length: 10 }, () => createWallet('1000.00')));
    const keys: string[] = [];
    for (let i = 0; i < 120; i++) {
      const w = wallets[i % 10]!;
      const body = { ...tx(w, { money: { amount: '1.00', currency: 'BRL' } }), idempotencyKey: '' };
      body.idempotencyKey = `${body.providerId}:${body.externalTransactionId}`;
      keys.push(body.idempotencyKey);
      await gateway.sendToInput(wagerMessage(body).body, w.id, crypto.randomUUID());
    }
    // SIGTERM em UMA réplica enquanto as mensagens são consumidas pelas três
    const victim = new TextDecoder().decode(Bun.spawnSync(['docker', 'compose', 'ps', '-q', 'app']).stdout).trim().split('\n')[0]!;
    const killedAt = new Date().toISOString();
    expect(Bun.spawnSync(['docker', 'kill', '--signal', 'SIGTERM', victim]).exitCode).toBe(0);

    await waitFor(async () => {
      const [row] = await orm.em
        .fork()
        .execute<{ n: string }[]>(
          `SELECT count(*)::text AS n FROM wager_transactions WHERE idempotency_key = ANY(?::text[]) AND status = 'PROCESSED'`,
          [`{${keys.map((k) => `"${k}"`).join(',')}}`],
        );
      return Number(row?.n) === keys.length || undefined;
    }, 120_000, 1_000);

    for (const w of wallets) {
      expect(await walletBalance(orm, w.id)).toBe('988.00');
      expect(await countRows(orm, `wallet_ledger_entries WHERE wallet_id = ? AND direction = 'DEBIT'`, [w.id])).toBe(12);
      await assertLedgerInvariant(orm, w.id);
    }

    // o shutdown foi cooperativo: consumer/workers parados pelo hook antes de o pool fechar
    const logs = new TextDecoder().decode(Bun.spawnSync(['docker', 'logs', '--since', killedAt, victim]).stdout);
    expect(logs).toContain('"signal":"SIGTERM"');
    expect(logs).toContain('background processes stopped');
    const exit = new TextDecoder().decode(Bun.spawnSync(['docker', 'inspect', '-f', '{{.State.ExitCode}}', victim]).stdout).trim();
    expect(exit).toBe('143'); // 128 + SIGTERM: saiu pelo sinal, não por crash nem SIGKILL

    // reinício (o Docker não reinicia após `docker kill`; em produção o orquestrador recria):
    // a instância volta, fica healthy e volta a atender sem nenhuma reconciliação manual.
    expect(Bun.spawnSync(['docker', 'start', victim]).exitCode).toBe(0);
    await waitFor(async () => {
      const state = new TextDecoder().decode(Bun.spawnSync(['docker', 'inspect', '-f', '{{.State.Health.Status}}', victim]).stdout);
      return state.trim() === 'healthy' || undefined;
    }, 90_000, 1_000);
    const hostname = new TextDecoder().decode(Bun.spawnSync(['docker', 'inspect', '-f', '{{.Config.Hostname}}', victim]).stdout).trim();
    await waitFor(async () => {
      const res = await fetch(`${BASE_URL}/health/live`);
      return res.headers.get('x-instance-id') === hostname || undefined;
    }, 30_000, 100);
  }, 240_000);
});
