/**
 * Reinício do serviço com comprovação da consistência final.
 * Processo REAL da aplicação (bun run src/main.ts, todos os papéis) morto com kill -9 no meio de
 * um disparo HTTP concorrente e do consumo SQS; o mesmo serviço sobe de novo e o cliente reenvia
 * tudo com as MESMAS Idempotency-Keys. Postgres e LocalStack reais.
 */
import { afterAll, beforeAll, expect, test } from 'bun:test';
import type { Subprocess } from 'bun';
import type { MikroORM } from '@mikro-orm/postgresql';
import { assertLedgerInvariant, barrier, countRows, walletBalance } from '../support/app-context';
import { createTestOrm } from '../support/db';
import { createTestQueues, type SqsGateway, waitFor, wagerMessage } from '../support/sqs';

const PORT = 3900 + Math.floor(Math.random() * 90);
const BASE_URL = `http://127.0.0.1:${PORT}`;
let orm: MikroORM;
let gateway: SqsGateway;
let names: { input: string; deadLetter: string; events: string };
let proc: Subprocess | undefined;

function startService(): Subprocess {
  return Bun.spawn(['bun', 'run', 'src/main.ts'], {
    env: {
      ...process.env,
      PORT: String(PORT),
      INSTANCE_ID: `restart-${PORT}`,
      LOG_LEVEL: 'silent',
      SQS_INPUT_QUEUE: names.input,
      SQS_DLQ: names.deadLetter,
      SQS_EVENTS_QUEUE: names.events,
      SQS_WAIT_TIME_SECONDS: '1',
      // timing coerente com a validação de startup: (3+1)×1s + backoff ≈ 4s × 1,5 < 7s
      DB_LOCK_TIMEOUT_MS: '1000',
      UOW_MAX_ATTEMPTS: '3',
      SQS_VISIBILITY_TIMEOUT_SECONDS: '7',
      OUTBOX_INTERVAL_MS: '100',
      OUTBOX_BATCH_SIZE: '100',
      PENDING_REFERENCE_BASE_DELAY_MS: '200',
      PENDING_REFERENCE_INTERVAL_MS: '100',
    },
    stdout: 'ignore',
    stderr: 'ignore',
  });
}

async function waitReady() {
  await waitFor(async () => {
    const res = await fetch(`${BASE_URL}/health/ready`).catch(() => undefined);
    return res?.ok || undefined;
  }, 60_000, 200);
}

async function post(path: string, body: unknown, headers: Record<string, string> = {}) {
  const res = await fetch(`${BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: JSON.stringify(body),
  });
  return { status: res.status, body: (await res.json()) as any };
}

beforeAll(async () => {
  orm = await createTestOrm(5);
  const queues = await createTestQueues('restart', 10);
  ({ gateway, names } = queues);
  proc = startService();
  await waitReady();
});
afterAll(async () => {
  proc?.kill(9);
  await proc?.exited;
  await orm.close(true);
});

test('kill -9 no meio da carga → restart → reenvio com as mesmas keys → consistência final', async () => {
  const WALLETS = 10;
  const wallets = await Promise.all(
    Array.from({ length: WALLETS }, async () =>
      (await post('/wallets', { playerId: crypto.randomUUID(), initialBalance: { amount: '1000.00', currency: 'BRL' } })).body as {
        id: string;
        playerId: string;
      },
    ),
  );
  const tx = (w: { id: string; playerId: string }, overrides: Record<string, unknown>) => {
    const externalTransactionId = `rs-${crypto.randomUUID()}`;
    return {
      providerId: 'provider-a',
      externalTransactionId,
      idempotencyKey: `provider-a:${externalTransactionId}`,
      playerId: w.playerId,
      walletId: w.id,
      roundId: 'r',
      gameId: 'g',
      kind: 'BET',
      money: { amount: '1.00', currency: 'BRL' },
      ...overrides,
    };
  };

  // Por wallet: 30 BET HTTP de 1.00, 6 BET SQS de 2.00 e um REFUND (5.00) entregue ANTES da BET.
  const httpBets = wallets.flatMap((w) => Array.from({ length: 30 }, () => tx(w, {})));
  const sqsOps = wallets.flatMap((w) => {
    const bet = tx(w, { money: { amount: '5.00', currency: 'BRL' } });
    const refund = tx(w, { kind: 'REFUND', money: { amount: '5.00', currency: 'BRL' }, referenceExternalTransactionId: bet.externalTransactionId });
    return [refund, ...Array.from({ length: 6 }, () => tx(w, { money: { amount: '2.00', currency: 'BRL' } })), bet];
  });
  for (const op of sqsOps) await gateway.sendToInput(wagerMessage(op).body, op.walletId, crypto.randomUUID());

  // Disparo concorrente; o processo morre (kill -9: sem shutdown gracioso) no meio dele.
  const ready = barrier(httpBets.length);
  const submit = (b: (typeof httpBets)[number]) =>
    post('/wagering/transactions', (({ idempotencyKey: _k, ...body }) => body)(b), { 'idempotency-key': b.idempotencyKey });
  const inFlight = httpBets.map(async (b) => {
    await ready();
    return submit(b).catch(() => ({ status: 0, body: undefined }));
  });
  // Mata o processo no MEIO: quando parte das apostas já foi confirmada e o resto ainda está em voo.
  const httpKeys = `{${httpBets.map((b) => `"${b.idempotencyKey}"`).join(',')}}`;
  await waitFor(async () => {
    const committed = await countRows(orm, `wager_transactions WHERE idempotency_key = ANY(?::text[])`, [httpKeys]);
    return committed >= 60 || undefined;
  }, 30_000, 5);
  proc!.kill(9);
  await proc!.exited;
  const firstPass = await Promise.all(inFlight);
  const lost = firstPass.filter((r) => r.status === 0).length;
  expect(lost).toBeGreaterThan(0); // houve requisições interrompidas pelo kill
  expect(firstPass.filter((r) => r.status === 200).length).toBeGreaterThan(0);

  // Restart do serviço.
  proc = startService();
  await waitReady();

  // O provedor reenvia TUDO com as mesmas keys: o que já tinha sido confirmado vira replay.
  const secondPass = await Promise.all(httpBets.map(submit));
  expect(secondPass.every((r) => r.status === 200)).toBe(true);
  const replays = secondPass.filter((r) => r.body.idempotentReplay).length;
  const answered = firstPass.filter((r) => r.status === 200).length;
  // Tudo que foi respondido antes do kill vira replay. Pode haver MAIS replays que respostas:
  // commit confirmado e processo morto antes de responder. O cliente viu erro de rede, reenviou
  // com a mesma key e recebeu o resultado original, sem segundo débito.
  expect(replays).toBeGreaterThanOrEqual(answered);
  const committedButResponseLost = replays - answered;

  // Mensagens SQS (inclusive as que estavam em voo no processo morto) e o REFUND fora de ordem.
  const keys = [...httpBets, ...sqsOps].map((o) => o.idempotencyKey);
  await waitFor(async () => {
    const [row] = await orm.em
      .fork()
      .execute<{ n: string }[]>(
        `SELECT count(*)::text AS n FROM wager_transactions WHERE idempotency_key = ANY(?::text[]) AND status = 'PROCESSED'`,
        [`{${keys.map((k) => `"${k}"`).join(',')}}`],
      );
    return Number(row?.n) === keys.length || undefined;
  }, 90_000, 500);

  for (const w of wallets) {
    expect(await walletBalance(orm, w.id)).toBe('958.00'); // 1000 − 30×1 − 6×2 − 5 + 5
    expect(await countRows(orm, `wallet_ledger_entries WHERE wallet_id = ?`, [w.id])).toBe(1 + 30 + 6 + 2);
    await assertLedgerInvariant(orm, w.id);
  }
  // nenhuma transação ficou presa; cada key existe exatamente uma vez
  expect(await countRows(orm, `wager_transactions WHERE wallet_id = ANY(?::uuid[]) AND status IN ('PENDING','PENDING_REFERENCE')`, [`{${wallets.map((w) => w.id).join(',')}}`])).toBe(0);
  expect(await countRows(orm, `wager_transactions WHERE idempotency_key = ANY(?::text[])`, [`{${keys.map((k) => `"${k}"`).join(',')}}`])).toBe(keys.length);
  // a outbox dessas wallets é escoada pelo processo reiniciado (eventos confirmados não se perdem)
  const ids = `{${wallets.map((w) => w.id).join(',')}}`;
  await waitFor(async () => {
    const pending = await countRows(
      orm,
      `outbox_messages WHERE published_at IS NULL AND (aggregate_id = ANY(?::uuid[]) OR aggregate_id IN (SELECT id FROM wager_transactions WHERE wallet_id = ANY(?::uuid[])))`,
      [ids, ids],
    );
    return pending === 0 || undefined;
  }, 90_000, 500);

  console.log(JSON.stringify({ restartTest: { httpRequests: httpBets.length, answeredBeforeKill: answered, lostOnKill: lost, replaysAfterRestart: replays, committedButResponseLost } }));
}, 240_000);
