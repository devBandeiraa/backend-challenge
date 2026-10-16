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
import { ledgerInvariantAfterEach } from '../support/invariants';
import { createTestOrm } from '../support/db';
import { waitFor, wagerMessage } from '../support/sqs';

const BASE_URL = process.env.MULTI_INSTANCE_URL ?? 'http://localhost:8088';
let orm: MikroORM;
let gateway: SqsGateway;
const touch = ledgerInvariantAfterEach(() => orm);

beforeAll(async () => {
  const health = await fetch(`${BASE_URL}/health/ready`).catch(() => undefined);
  if (!health?.ok) throw new Error(`stack not ready at ${BASE_URL}: run "bun run test:multi" (docker compose up --build --wait)`);
  orm = await createTestOrm(5);
  gateway = await SqsGateway.connect(createSqsClient(), DEFAULT_QUEUE_NAMES);
});
afterAll(async () => {
  await orm?.close(true);
});

/**
 * Pelo host, o proxy de portas do Docker Desktop (Windows) derruba ~0,4% das conexões com
 * ECONNRESET aos 30s sem que a requisição chegue ao nginx (docs/load-test-analysis.md).
 * Requisições COM Idempotency-Key são reenviadas, como faria um provedor real: é exatamente o
 * caso de uso da key, e as asserções de "um único débito" continuam valendo.
 */
let transportRetries = 0;
async function post(path: string, body: unknown, headers: Record<string, string> = {}) {
  for (let attempt = 1; ; attempt++) {
    try {
      const res = await fetch(`${BASE_URL}${path}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...headers },
        body: JSON.stringify(body),
      });
      return { status: res.status, instance: res.headers.get('x-instance-id'), body: (await res.json()) as any };
    } catch (error) {
      if (!headers['idempotency-key'] || attempt >= 3 || (error as { code?: string }).code !== 'ECONNRESET') throw error;
      transportRetries += 1;
    }
  }
}
afterAll(() => {
  if (transportRetries > 0) console.log(JSON.stringify({ transportRetriesWithSameIdempotencyKey: transportRetries }));
});

async function createWallet(amount: string) {
  const res = await post('/wallets', { playerId: crypto.randomUUID(), initialBalance: { amount, currency: 'BRL' } });
  expect(res.status).toBe(201);
  touch(res.body.id);
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
    }, 45_000, 1_000); // < visibility (90 s): mensagem presa num poll faria o teste falhar

    for (const w of wallets) {
      expect(await walletBalance(orm, w.id)).toBe('988.00');
      expect(await countRows(orm, `wallet_ledger_entries WHERE wallet_id = ? AND direction = 'DEBIT'`, [w.id])).toBe(12);
      await assertLedgerInvariant(orm, w.id);
    }

    // o shutdown foi cooperativo: consumer/workers parados pelo hook antes de o pool fechar.
    // A vítima sai só depois de o long poll em andamento terminar (não é abortado): espera ela sair.
    await waitFor(async () => {
      const running = new TextDecoder().decode(Bun.spawnSync(['docker', 'inspect', '-f', '{{.State.Running}}', victim]).stdout).trim();
      return running === 'false' || undefined;
    }, 30_000, 200);
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

  test('SIGTERM durante o long poll: o que a réplica recebe é devolvido na hora e outra réplica processa em segundos', async () => {
    const wallets = await Promise.all(Array.from({ length: 30 }, () => createWallet('100.00')));
    const keys: string[] = [];
    const bodies = wallets.map((w) => {
      const body = { ...tx(w, { money: { amount: '3.00', currency: 'BRL' } }), idempotencyKey: '' };
      body.idempotencyKey = `${body.providerId}:${body.externalTransactionId}`;
      keys.push(body.idempotencyKey);
      return body;
    });
    // réplicas ociosas: as três estão bloqueadas num long poll (SQS_WAIT_TIME_SECONDS=5)
    const victim = new TextDecoder().decode(Bun.spawnSync(['docker', 'compose', 'ps', '-q', 'app']).stdout).trim().split('\n')[0]!;
    const killedAt = new Date().toISOString();
    expect(Bun.spawnSync(['docker', 'kill', '--signal', 'SIGTERM', victim]).exitCode).toBe(0);
    const sentAt = performance.now();
    // chegam enquanto o long poll da vítima ainda está aberto (ele não é abortado)
    await Promise.all(bodies.map((b) => gateway.sendToInput(wagerMessage(b).body, b.walletId, crypto.randomUUID())));

    await waitFor(async () => {
      const [row] = await orm.em
        .fork()
        .execute<{ n: string }[]>(
          `SELECT count(*)::text AS n FROM wager_transactions WHERE idempotency_key = ANY(?::text[]) AND status = 'PROCESSED'`,
          [`{${keys.map((k) => `"${k}"`).join(',')}}`],
        );
      return Number(row?.n) === keys.length || undefined;
    }, 30_000, 200);
    const allProcessedMs = Math.round(performance.now() - sentAt);
    // uma mensagem presa no poll da vítima só voltaria após o visibility timeout (90 s)
    expect(allProcessedMs).toBeLessThan(20_000);
    for (const w of wallets) expect(await walletBalance(orm, w.id)).toBe('97.00');

    await waitFor(async () => {
      const running = new TextDecoder().decode(Bun.spawnSync(['docker', 'inspect', '-f', '{{.State.Running}}', victim]).stdout).trim();
      return running === 'false' || undefined;
    }, 30_000, 200);
    const logs = new TextDecoder().decode(Bun.spawnSync(['docker', 'logs', '--since', killedAt, victim]).stdout);
    const releasedByVictim = logs.split('\n').filter((l) => l.includes('"message released on shutdown"')).length;
    expect(logs).toContain('background processes stopped');
    expect(new TextDecoder().decode(Bun.spawnSync(['docker', 'inspect', '-f', '{{.State.ExitCode}}', victim]).stdout).trim()).toBe('143');
    console.log(JSON.stringify({ sigtermDuringLongPoll: { messages: keys.length, allProcessedMs, releasedByVictim } }));

    expect(Bun.spawnSync(['docker', 'start', victim]).exitCode).toBe(0);
    await waitFor(async () => {
      const state = new TextDecoder().decode(Bun.spawnSync(['docker', 'inspect', '-f', '{{.State.Health.Status}}', victim]).stdout);
      return state.trim() === 'healthy' || undefined;
    }, 90_000, 1_000);
  }, 180_000);
});

/** Executa um comando docker e devolve a saída (stdout) como texto. */
const sh = (cmd: string[]) => new TextDecoder().decode(Bun.spawnSync(cmd).stdout).trim();
const replicaIds = () => sh(['docker', 'compose', 'ps', '-q', 'app']).split('\n').filter(Boolean);
/** /health/ready de CADA réplica, direto no container (o nginx esconderia uma réplica travada). */
const readyByReplica = () =>
  replicaIds().map((id) => {
    const out = Bun.spawnSync(['docker', 'exec', id, 'wget', '-qO-', '-T', '3', 'http://localhost:3000/health/ready']);
    return { id, ready: out.exitCode === 0 && new TextDecoder().decode(out.stdout).includes('"status":"ok"') };
  });

// Por último no arquivo: derruba o Postgres do stack.
describe('queda do Postgres (30 s) com carga', () => {
  /** Prazo para as 3 réplicas voltarem a ready sozinhas depois que o Postgres fica healthy. */
  const RECOVERY_DEADLINE_MS = 60_000;

  test('as 3 réplicas voltam a ready sozinhas, sem 5xx depois disso, e a invariante bate', async () => {
    expect(readyByReplica().every((r) => r.ready)).toBe(true);
    const wallets = await Promise.all(Array.from({ length: 10 }, () => createWallet('1000.00')));

    // carga contínua pelo nginx durante toda a queda (cada requisição com a própria key)
    let loading = true;
    const sent: { body: ReturnType<typeof tx>; status: number }[] = [];
    const load = Promise.all(
      Array.from({ length: 10 }, async (_, i) => {
        while (loading) {
          const body = tx(wallets[i]!, { money: { amount: '1.00', currency: 'BRL' } });
          const res = await submit(body).catch(() => ({ status: -1 }) as { status: number });
          sent.push({ body, status: res.status });
          await Bun.sleep(100);
        }
      }),
    );
    await Bun.sleep(2_000);
    expect(Bun.spawnSync(['docker', 'compose', 'stop', '-t', '0', 'postgres']).exitCode).toBe(0);
    await Bun.sleep(30_000);
    expect(Bun.spawnSync(['docker', 'compose', 'start', 'postgres']).exitCode).toBe(0);
    await waitFor(async () => (sh(['docker', 'inspect', '-f', '{{.State.Health.Status}}', sh(['docker', 'compose', 'ps', '-q', 'postgres'])]) === 'healthy') || undefined, 60_000, 500);
    const healthyAt = performance.now();

    // as 3 réplicas voltam a ready SEM reinício (mesmo container, mesmo processo)
    const before = new Map(replicaIds().map((id) => [id, sh(['docker', 'inspect', '-f', '{{.State.StartedAt}}', id])]));
    await waitFor(async () => readyByReplica().every((r) => r.ready) || undefined, RECOVERY_DEADLINE_MS, 1_000);
    const recoveredMs = Math.round(performance.now() - healthyAt);
    for (const [id, startedAt] of before) expect(sh(['docker', 'inspect', '-f', '{{.State.StartedAt}}', id])).toBe(startedAt);
    loading = false;
    await load;

    // depois da recuperação: nenhuma resposta 5xx, nas 3 réplicas
    const after = await burst(60, (i) => submit(tx(wallets[i % 10]!, { money: { amount: '1.00', currency: 'BRL' } })));
    expect(after.filter((r) => r.status >= 500)).toEqual([]);
    expect(after.every((r) => r.status === 200)).toBe(true);
    expect(new Set(after.map((r) => r.instance)).size).toBeGreaterThanOrEqual(3);

    // o consumer de cada réplica também voltou: mensagens novas são processadas
    const keys: string[] = [];
    for (const w of wallets) {
      const body = { ...tx(w, { money: { amount: '1.00', currency: 'BRL' } }), idempotencyKey: '' };
      body.idempotencyKey = `${body.providerId}:${body.externalTransactionId}`;
      keys.push(body.idempotencyKey);
      await gateway.sendToInput(wagerMessage(body).body, w.id, crypto.randomUUID());
    }
    await waitFor(async () => {
      const [row] = await orm.em.fork().execute<{ n: string }[]>(
        `SELECT count(*)::text AS n FROM wager_transactions WHERE idempotency_key = ANY(?::text[]) AND status = 'PROCESSED'`,
        [`{${keys.map((k) => `"${k}"`).join(',')}}`],
      );
      return Number(row?.n) === keys.length || undefined;
    }, 30_000, 500);

    // o que a carga recebeu sem resposta definitiva é reenviado com a MESMA key (como faria o provedor)
    const unresolved = sent.filter((s) => ![200, 422].includes(s.status));
    for (const s of unresolved) {
      const res = await submit(s.body);
      expect(res.status).toBe(200);
    }
    // cada BET aceita debitou exatamente uma vez: saldo = 1000 − nº de BETs PROCESSED; ledger bate (afterEach)
    for (const w of wallets) {
      const processed = await countRows(orm, `wager_transactions WHERE wallet_id = ? AND kind = 'BET' AND status = 'PROCESSED'`, [w.id]);
      const debits = await countRows(orm, `wallet_ledger_entries WHERE wallet_id = ? AND direction = 'DEBIT'`, [w.id]);
      expect(debits).toBe(processed);
      expect(await walletBalance(orm, w.id)).toBe((1000 - processed).toFixed(2));
    }
    console.log(JSON.stringify({ postgresOutage: { requestsDuringLoad: sent.length, unresolvedResent: unresolved.length, recoveredMs, statuses: Object.fromEntries([...new Set(sent.map((s) => s.status))].map((st) => [st, sent.filter((s) => s.status === st).length])) } }));
  }, 240_000);
});
