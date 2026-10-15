/**
 * Gerador de carga. Roda DENTRO da rede do compose (serviço `loadgen`), batendo no nginx por
 * nome de serviço: a medição não passa pelo encaminhamento de portas do Docker Desktop, que no
 * Windows derruba ~0,4% das conexões com ECONNRESET aos 30s (ver ARCHITECTURE.md).
 * Imprime uma linha `RESULTS_JSON:{...}` que o orquestrador (scripts/load-test.ts) transforma em relatório.
 */
import { SendMessageBatchCommand } from '@aws-sdk/client-sqs';
import { MikroORM } from '@mikro-orm/postgresql';
import { buildOrmConfig } from '../src/infrastructure/persistence/mikro-orm.config';
import { createSqsClient, DEFAULT_QUEUE_NAMES } from '../src/infrastructure/messaging/sqs/sqs-config';
import { SqsGateway } from '../src/infrastructure/messaging/sqs/sqs-gateway';

const env = (k: string, d: number) => Number(process.env[k] ?? d);
export const CFG = {
  baseUrl: process.env.LOAD_BASE_URL ?? 'http://localhost:8088',
  durationS: env('LOAD_DURATION_S', 30),
  concurrency: env('LOAD_CONCURRENCY', 50),
  wallets: env('LOAD_WALLETS', 200),
  hotShare: env('LOAD_HOT_SHARE', 0.2),
  replayShare: env('LOAD_REPLAY_SHARE', 0.05),
  sqsMessages: env('LOAD_SQS_MESSAGES', 2000),
};

const pct = (sorted: number[], p: number) => sorted[Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1)] ?? 0;
const round = (n: number, d = 1) => Math.round(n * 10 ** d) / 10 ** d;
const summary = (values: number[]) => {
  const s = [...values].sort((a, b) => a - b);
  return { count: s.length, p50: round(pct(s, 50)), p95: round(pct(s, 95)), p99: round(pct(s, 99)), max: round(s.at(-1) ?? 0) };
};

interface Wallet {
  id: string;
  playerId: string;
}

async function post(path: string, body: unknown, headers: Record<string, string> = {}) {
  const res = await fetch(`${CFG.baseUrl}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: JSON.stringify(body),
  });
  return { status: res.status, body: (await res.json().catch(() => undefined)) as any };
}

/** Soma um counter nas réplicas: raspa /metrics pelo nginx até ter visto todas as instâncias. */
async function scrape(metric: string, replicas: number): Promise<number> {
  const perInstance = new Map<string, number>();
  for (let i = 0; i < 100 && perInstance.size < replicas; i++) {
    const res = await fetch(`${CFG.baseUrl}/metrics`);
    const text = await res.text();
    const total = text
      .split('\n')
      .filter((l) => l.startsWith(metric) && !l.startsWith('#'))
      .reduce((s, l) => s + Number(l.split(' ').at(-1)), 0);
    perInstance.set(res.headers.get('x-instance-id') ?? `?${i}`, total);
  }
  return [...perInstance.values()].reduce((a, b) => a + b, 0);
}

async function main() {
  const replicas = env('LOAD_REPLICAS', 3);
  const orm = await MikroORM.init(buildOrmConfig({ poolMax: 4 }));
  const db = () => orm.em.fork();
  const gateway = await SqsGateway.connect(createSqsClient(), DEFAULT_QUEUE_NAMES);
  const sqs = createSqsClient();
  const count = async (sql: string, params: unknown[] = []) =>
    Number((await db().execute<{ n: string }[]>(`SELECT count(*)::text AS n FROM ${sql}`, params))[0]?.n ?? 0);

  // ---- preparação
  const createWallet = async (amount: string): Promise<Wallet> =>
    (await post('/wallets', { playerId: crypto.randomUUID(), initialBalance: { amount, currency: 'BRL' } })).body;
  const wallets: Wallet[] = [];
  for (let i = 0; i < CFG.wallets; i += 20) {
    wallets.push(...(await Promise.all(Array.from({ length: Math.min(20, CFG.wallets - i) }, () => createWallet('1000000.00')))));
  }
  const hot = await createWallet('100000.00');
  const allWallets = [...wallets, hot];

  // Backlog de execuções anteriores distorceria o outbox lag: espera escoar antes de medir.
  const backlog = await count(`outbox_messages WHERE published_at IS NULL`);
  for (let i = 0; i < 240 && (await count(`outbox_messages WHERE published_at IS NULL`)) > 0; i++) await Bun.sleep(500);

  const lockBefore = await scrape('wagering_db_lock_conflicts_total', replicas);
  const exhaustedBefore = await scrape('wagering_db_retries_exhausted_total', replicas);

  // ---- outbox lag amostrado a cada 1s (relógio do banco) durante as duas fases
  const lagSamples: number[] = [];
  let sampling = true;
  const sampler = (async () => {
    while (sampling) {
      const [row] = await db().execute<{ lag: string | null }[]>(
        `SELECT EXTRACT(EPOCH FROM now() - min(occurred_at))::text AS lag FROM outbox_messages WHERE published_at IS NULL`,
      );
      lagSamples.push(row?.lag ? Number(row.lag) : 0);
      await Bun.sleep(1_000);
    }
  })();

  // ---- fase 1: HTTP em malha fechada
  type Req = { key: string; body: Record<string, unknown> };
  const sent: Req[] = [];
  const latencies: number[] = [];
  const hotLatencies: number[] = [];
  const otherLatencies: number[] = [];
  const byStatus: Record<string, number> = {};
  const business: Record<string, number> = {};
  let replays = 0;
  let networkErrors = 0;
  const deadline = Date.now() + CFG.durationS * 1_000;
  const startedAt = performance.now();

  const nextRequest = (): Req => {
    if (sent.length > 0 && Math.random() < CFG.replayShare) return sent[Math.floor(Math.random() * sent.length)]!;
    const w = Math.random() < CFG.hotShare ? hot : wallets[Math.floor(Math.random() * wallets.length)]!;
    const r = Math.random();
    const kind = r < 0.8 ? 'BET' : r < 0.95 ? 'WIN' : 'LOSS';
    const ext = `load-${crypto.randomUUID()}`;
    const req = {
      key: `provider-load:${ext}`,
      body: {
        providerId: 'provider-load',
        externalTransactionId: ext,
        playerId: w.playerId,
        walletId: w.id,
        roundId: 'r',
        gameId: 'g',
        kind,
        money: { amount: kind === 'WIN' ? '2.00' : '1.00', currency: 'BRL' },
      },
    };
    sent.push(req);
    return req;
  };

  await Promise.all(
    Array.from({ length: CFG.concurrency }, async () => {
      while (Date.now() < deadline) {
        const req = nextRequest();
        const t0 = performance.now();
        try {
          const res = await post('/wagering/transactions', req.body, { 'idempotency-key': req.key });
          const ms = performance.now() - t0;
          latencies.push(ms);
          (req.body.walletId === hot.id ? hotLatencies : otherLatencies).push(ms);
          byStatus[res.status] = (byStatus[res.status] ?? 0) + 1;
          if (res.body?.idempotentReplay) replays += 1;
          if (res.status === 422) business[res.body?.failureCode] = (business[res.body?.failureCode] ?? 0) + 1;
        } catch {
          networkErrors += 1;
        }
      }
    }),
  );
  const httpSeconds = (performance.now() - startedAt) / 1000;

  // ---- fase 2: SQS, consumers das réplicas competindo
  const sqsKeys: string[] = [];
  const entries = Array.from({ length: CFG.sqsMessages }, (_, i) => {
    const w = wallets[i % wallets.length]!;
    const ext = `load-sqs-${crypto.randomUUID()}`;
    const idempotencyKey = `provider-load:${ext}`;
    sqsKeys.push(idempotencyKey);
    const messageId = `msg-${crypto.randomUUID()}`;
    const data = { providerId: 'provider-load', externalTransactionId: ext, idempotencyKey, playerId: w.playerId, walletId: w.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '1.00', currency: 'BRL' } };
    return { MessageBody: JSON.stringify({ messageId, type: 'WagerTransactionRequested', data }), MessageGroupId: w.id, MessageDeduplicationId: messageId };
  });
  const sqsStart = performance.now();
  for (let i = 0; i < entries.length; i += 10) {
    await sqs.send(new SendMessageBatchCommand({ QueueUrl: gateway.urls.input, Entries: entries.slice(i, i + 10).map((e, j) => ({ ...e, Id: String(j) })) }));
  }
  const enqueueSeconds = (performance.now() - sqsStart) / 1000;
  const keyArray = `{${sqsKeys.map((k) => `"${k}"`).join(',')}}`;
  while ((await count(`wager_transactions WHERE idempotency_key = ANY(?::text[]) AND status = 'PROCESSED'`, [keyArray])) < sqsKeys.length) {
    if ((performance.now() - sqsStart) / 1000 > 300) throw new Error('SQS phase did not drain in 300s');
    await Bun.sleep(250);
  }
  const sqsSeconds = (performance.now() - sqsStart) / 1000;

  // ---- escoamento da outbox após a carga
  const loadEnd = performance.now();
  while ((await count(`outbox_messages WHERE published_at IS NULL`)) > 0 && (performance.now() - loadEnd) / 1000 < 300) await Bun.sleep(250);
  const outboxDrainSeconds = (performance.now() - loadEnd) / 1000;
  sampling = false;
  await sampler;

  const lockConflicts = (await scrape('wagering_db_lock_conflicts_total', replicas)) - lockBefore;
  const retriesExhausted = (await scrape('wagering_db_retries_exhausted_total', replicas)) - exhaustedBefore;

  // ---- consistência final de todas as wallets da carga
  const ids = `{${allWallets.map((w) => w.id).join(',')}}`;
  const mismatches = (
    await db().execute<{ id: string }[]>(
      `SELECT w.id FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id = w.id
        WHERE w.id = ANY(?::uuid[]) GROUP BY w.id
       HAVING w.balance <> COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END), 0)
           OR w.version <> COALESCE(MAX(l.wallet_version), 1)`,
      [ids],
    )
  ).length;
  const duplicated = await count(
    `(SELECT transaction_id FROM wallet_ledger_entries WHERE wallet_id = ANY(?::uuid[]) GROUP BY transaction_id HAVING count(*) > 1) d`,
    [ids],
  );
  const negative = await count(`wallets WHERE id = ANY(?::uuid[]) AND balance < 0`, [ids]);
  await orm.close(true);

  const lat = latencies.sort((a, b) => a - b);
  const serverErrors = Object.entries(byStatus).filter(([s]) => Number(s) >= 500).reduce((a, [, n]) => a + n, 0);
  const total = latencies.length + networkErrors;
  return {
    config: CFG,
    outboxBacklogBeforeRun: backlog,
    http: {
      requests: total,
      durationS: round(httpSeconds),
      throughputRps: round(latencies.length / httpSeconds),
      latencyMs: { p50: round(pct(lat, 50)), p95: round(pct(lat, 95)), p99: round(pct(lat, 99)), max: round(lat.at(-1) ?? 0) },
      latencyByTarget: { hotWallet: summary(hotLatencies), otherWallets: summary(otherLatencies) },
      byStatus,
      replays,
      businessRejections: business,
      errors: { http5xx: serverErrors, network: networkErrors, rate: round(((serverErrors + networkErrors) / Math.max(1, total)) * 100, 3) },
    },
    sqs: { messages: CFG.sqsMessages, enqueueS: round(enqueueSeconds), endToEndS: round(sqsSeconds), throughputMsgPerS: round(CFG.sqsMessages / sqsSeconds) },
    concurrency: { lockConflictRetries: lockConflicts, retriesExhausted },
    outbox: {
      maxLagS: round(Math.max(0, ...lagSamples), 2),
      p95LagS: round(pct([...lagSamples].sort((a, b) => a - b), 95), 2),
      drainAfterLoadS: round(outboxDrainSeconds),
    },
    consistency: { walletsChecked: allWallets.length, ledgerMismatches: mismatches, duplicatedLedgerEntries: duplicated, negativeBalances: negative },
  };
}

if (import.meta.main) {
  const results = await main();
  console.log(`RESULTS_JSON:${JSON.stringify(results)}`);
  process.exit(0);
}
