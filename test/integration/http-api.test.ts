import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';

import { createTestOrm } from '../support/db';
import { ledgerInvariantAfterEach } from '../support/invariants';
import { http, startHttpApp } from '../support/http-app';

let baseUrl: string;
let close: () => Promise<void>;
let orm: MikroORM;
const touch = ledgerInvariantAfterEach(() => orm);

beforeAll(async () => {
  ({ baseUrl, close } = await startHttpApp());
  orm = await createTestOrm(2);
});
afterAll(async () => {
  await close();
  await orm.close(true);
});

const call = (method: string, path: string, options?: Parameters<typeof http>[3]) => http(baseUrl, method, path, options);

async function createWallet(amount = '1000.00') {
  const res = await call('POST', '/wallets', { body: { playerId: crypto.randomUUID(), initialBalance: { amount, currency: 'BRL' } } });
  expect(res.status).toBe(201);
  touch(res.body.id);
  return res.body as { id: string; playerId: string };
}

function txBody(wallet: { id: string; playerId: string }, overrides: Record<string, unknown> = {}) {
  return {
    providerId: 'provider-a',
    externalTransactionId: `transaction-${crypto.randomUUID()}`,
    playerId: wallet.playerId,
    walletId: wallet.id,
    roundId: 'round-987',
    gameId: 'fortune-chimp',
    kind: 'BET',
    money: { amount: '25.00', currency: 'BRL' },
    ...overrides,
  };
}

const submit = (body: Record<string, unknown>, key = `${body.providerId}:${body.externalTransactionId}`) =>
  call('POST', '/wagering/transactions', { body, headers: key ? { 'idempotency-key': key } : {} });

describe('POST /wallets', () => {
  test('201 com o contrato do enunciado', async () => {
    const playerId = crypto.randomUUID();
    const res = await call('POST', '/wallets', { body: { playerId, initialBalance: { amount: '1000.00', currency: 'BRL' } } });
    expect(res.status).toBe(201);
    expect(res.body).toEqual({ id: expect.any(String), playerId, balance: { amount: '1000.00', currency: 'BRL' }, version: 1 });
    touch(res.body.id);
  });

  test('409 em wallet duplicada (player + moeda)', async () => {
    const w = await createWallet();
    const res = await call('POST', '/wallets', { body: { playerId: w.playerId, initialBalance: { amount: '1.00', currency: 'BRL' } } });
    expect(res.status).toBe(409);
    expect(res.body.error.code).toBe('WALLET_ALREADY_EXISTS');
  });

  test.each([
    ['amount como number', { playerId: crypto.randomUUID(), initialBalance: { amount: 10, currency: 'BRL' } }],
    ['amount com 3 casas', { playerId: crypto.randomUUID(), initialBalance: { amount: '1.001', currency: 'BRL' } }],
    ['amount negativo', { playerId: crypto.randomUUID(), initialBalance: { amount: '-1.00', currency: 'BRL' } }],
    ['playerId não-uuid', { playerId: 'abc', initialBalance: { amount: '1.00', currency: 'BRL' } }],
    ['moeda inexistente (XYZ)', { playerId: crypto.randomUUID(), initialBalance: { amount: '1.00', currency: 'XYZ' } }],
    ['moeda com 0 casas (JPY)', { playerId: crypto.randomUUID(), initialBalance: { amount: '1.00', currency: 'JPY' } }],
    ['campo desconhecido', { playerId: crypto.randomUUID(), initialBalance: { amount: '1.00', currency: 'BRL' }, extra: 1 }],
  ])('400 para %s', async (_label, body) => {
    const res = await call('POST', '/wallets', { body });
    expect(res.status).toBe(400);
    expect(res.body.error.code).toMatch(/INVALID_PAYLOAD|INVALID_MONEY/);
  });
});

describe('POST /wagering/transactions', () => {
  test('200 PROCESSED com o contrato do enunciado; replay idêntico devolve a mesma resposta', async () => {
    const w = await createWallet('1000.00');
    const body = txBody(w);
    const first = await submit(body);
    expect(first.status).toBe(200);
    expect(first.body).toEqual({
      transactionId: expect.any(String),
      status: 'PROCESSED',
      balance: { amount: '975.00', currency: 'BRL' },
      idempotentReplay: false,
    });
    const replay = await submit(body);
    expect(replay.status).toBe(200);
    expect(replay.body).toEqual({ ...first.body, idempotentReplay: true });
  });

  test('400 sem Idempotency-Key', async () => {
    const w = await createWallet();
    const res = await submit(txBody(w), '');
    expect(res.status).toBe(400);
    expect(res.body.error.details[0].message).toContain('Idempotency-Key');
  });

  test('409 com a mesma key e payload diferente; 409 com mesmo externalId e outra key', async () => {
    const w = await createWallet();
    const body = txBody(w);
    await submit(body);
    const mismatch = await submit({ ...body, money: { amount: '26.00', currency: 'BRL' } });
    expect([mismatch.status, mismatch.body.error.code]).toEqual([409, 'IDEMPOTENCY_KEY_PAYLOAD_MISMATCH']);
    const otherKey = await submit(body, 'some-other-key');
    expect([otherKey.status, otherKey.body.error.code]).toEqual([409, 'EXTERNAL_TRANSACTION_ID_CONFLICT']);
  });

  test('Idempotency-Key tem escopo por provedor: provider-b com a MESMA key de provider-a é processado normalmente', async () => {
    const w = await createWallet('100.00');
    const key = `shared-key-${crypto.randomUUID()}`;
    const a = await submit(txBody(w, { providerId: 'provider-a', money: { amount: '10.00', currency: 'BRL' } }), key);
    const b = await submit(txBody(w, { providerId: 'provider-b', money: { amount: '20.00', currency: 'BRL' } }), key);
    expect([a.status, a.body.status, a.body.idempotentReplay]).toEqual([200, 'PROCESSED', false]);
    expect([b.status, b.body.status, b.body.idempotentReplay]).toEqual([200, 'PROCESSED', false]);
    expect(b.body.transactionId).not.toBe(a.body.transactionId);
    expect(b.body.balance.amount).toBe('70.00');
    // cada provedor continua com o próprio replay
    const replayB = await submit(txBody(w, { providerId: 'provider-b', externalTransactionId: (await call('GET', `/wagering/transactions/${b.body.transactionId}`)).body.externalTransactionId, money: { amount: '20.00', currency: 'BRL' } }), key);
    expect([replayB.status, replayB.body.transactionId, replayB.body.idempotentReplay]).toEqual([200, b.body.transactionId, true]);
  });

  test('422 com failureCode em rejeição de negócio (também no replay)', async () => {
    const w = await createWallet('10.00');
    const body = txBody(w, { money: { amount: '10.01', currency: 'BRL' } });
    const res = await submit(body);
    expect(res.status).toBe(422);
    expect(res.body).toMatchObject({ status: 'REJECTED', failureCode: 'INSUFFICIENT_FUNDS', balance: { amount: '10.00' } });
    expect((await submit(body)).status).toBe(422);
  });

  test('422 BALANCE_LIMIT_EXCEEDED quando o crédito estouraria NUMERIC(19,2) (não 500); replay idem', async () => {
    const w = await createWallet('999.99');
    const body = txBody(w, { kind: 'WIN', money: { amount: '99999999999999999.99', currency: 'BRL' } });
    const res = await submit(body);
    expect(res.status).toBe(422);
    expect(res.body).toMatchObject({ status: 'REJECTED', failureCode: 'BALANCE_LIMIT_EXCEEDED', balance: { amount: '999.99' } });
    const replay = await submit(body);
    expect(replay.status).toBe(422);
    expect(replay.body).toEqual({ ...res.body, idempotentReplay: true });
    expect((await call('GET', `/wallets/${w.id}`)).body.balance.amount).toBe('999.99');
  });

  test('202 PENDING_REFERENCE quando a referência ainda não chegou', async () => {
    const w = await createWallet();
    const res = await submit(txBody(w, { kind: 'REFUND', referenceExternalTransactionId: 'not-yet' }));
    expect(res.status).toBe(202);
    expect(res.body).toMatchObject({ status: 'PENDING_REFERENCE', balance: null, idempotentReplay: false });
  });

  test.each([['internal'], ['Internal']])('400 para providerId reservado %p (mesmo com wallet inexistente)', async (providerId) => {
    const res = await submit(txBody({ id: crypto.randomUUID(), playerId: crypto.randomUUID() }, { providerId }));
    expect(res.status).toBe(400);
    expect(res.body.error.code).toBe('INVALID_PAYLOAD');
    expect(JSON.stringify(res.body.error.details)).toContain('reserved');
  });

  test('422 WALLET_NOT_FOUND para wallet inexistente', async () => {
    const res = await submit(txBody({ id: crypto.randomUUID(), playerId: crypto.randomUUID() }));
    expect(res.status).toBe(422);
    expect(res.body).toMatchObject({ status: 'REJECTED', failureCode: 'WALLET_NOT_FOUND', transactionId: null });
  });

  test.each<[string, number, (w: { id: string; playerId: string }) => Record<string, unknown>]>([
    ['PROCESSED', 200, (w) => txBody(w)],
    ['REJECTED', 422, (w) => txBody(w, { money: { amount: '999999.00', currency: 'BRL' } })],
    ['PENDING_REFERENCE', 202, (w) => txBody(w, { kind: 'ROLLBACK', referenceExternalTransactionId: 'missing' })],
    ['WALLET_NOT_FOUND', 422, () => txBody({ id: crypto.randomUUID(), playerId: crypto.randomUUID() })],
  ])('replay devolve o MESMO status HTTP e o mesmo corpo da primeira resposta: %s → %d', async (_label, expected, build) => {
    const body = build(await createWallet());
    const first = await submit(body);
    const replay = await submit(body);
    expect([first.status, replay.status]).toEqual([expected, expected]);
    const { idempotentReplay: _a, ...firstRest } = first.body;
    const { idempotentReplay: _b, ...replayRest } = replay.body;
    expect(replayRest).toEqual(firstRest);
  });

  test.each([
    ['valor sem casas decimais ("25")', { money: { amount: '25', currency: 'BRL' } }],
    ['valor com uma casa ("25.5")', { money: { amount: '25.5', currency: 'BRL' } }],
    ['kind OPENING (interno)', { kind: 'OPENING' }],
    ['REFUND sem referência', { kind: 'REFUND' }],
    ['valor em notação científica', { money: { amount: '1e2', currency: 'BRL' } }],
    ['valor como number', { money: { amount: 25, currency: 'BRL' } }],
    ['campo desconhecido', { refrenceExternalTransactionId: 'typo' }],
  ])('400 para %s', async (_label, overrides) => {
    const w = await createWallet();
    expect((await submit(txBody(w, overrides))).status).toBe(400);
  });

  test('400 para JSON malformado', async () => {
    const res = await call('POST', '/wagering/transactions', { rawBody: '{"kind":', headers: { 'idempotency-key': 'k' } });
    expect(res.status).toBe(400);
  });
});

describe('consultas', () => {
  test('GET /wallets/:id, 404 e 400', async () => {
    const w = await createWallet('50.00');
    const res = await call('GET', `/wallets/${w.id}`);
    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({ id: w.id, balance: { amount: '50.00', currency: 'BRL' }, version: 1 });
    expect((await call('GET', `/wallets/${crypto.randomUUID()}`)).status).toBe(404);
    expect((await call('GET', '/wallets/not-a-uuid')).status).toBe(400);
  });

  test('GET ledger: ordem decrescente, cursor opaco e estável mesmo com inserções entre páginas', async () => {
    const w = await createWallet('100.00');
    for (let i = 0; i < 4; i++) await submit(txBody(w, { money: { amount: '1.00', currency: 'BRL' } }));
    const page1 = await call('GET', `/wallets/${w.id}/ledger?limit=2`);
    expect(page1.body.entries.map((e: { walletVersion: number }) => e.walletVersion)).toEqual([5, 4]);
    // nova movimentação entre as páginas não desloca a próxima página (diferente de OFFSET)
    await submit(txBody(w, { money: { amount: '1.00', currency: 'BRL' } }));
    const page2 = await call('GET', `/wallets/${w.id}/ledger?limit=2&cursor=${page1.body.nextCursor}`);
    expect(page2.body.entries.map((e: { walletVersion: number }) => e.walletVersion)).toEqual([3, 2]);
    const page3 = await call('GET', `/wallets/${w.id}/ledger?limit=2&cursor=${page2.body.nextCursor}`);
    expect(page3.body.entries.map((e: { walletVersion: number }) => e.walletVersion)).toEqual([1]);
    expect(page3.body.nextCursor).toBeNull();
    // cursor de outra wallet é rejeitado
    const other = await createWallet('1.00');
    expect((await call('GET', `/wallets/${other.id}/ledger?cursor=${page1.body.nextCursor}`)).status).toBe(400);
  });

  test('GET transação por id e por (provider, externalId)', async () => {
    const w = await createWallet();
    const body = txBody(w);
    const { body: out } = await submit(body);
    const byId = await call('GET', `/wagering/transactions/${out.transactionId}`);
    expect(byId.body).toMatchObject({ transactionId: out.transactionId, status: 'PROCESSED', kind: 'BET', money: { amount: '25.00' } });
    const byExt = await call('GET', `/providers/provider-a/wagering/transactions/${body.externalTransactionId}`);
    expect(byExt.body.transactionId).toBe(out.transactionId);
    expect((await call('GET', `/providers/provider-a/wagering/transactions/nope`)).status).toBe(404);
  });
});

describe('transversais', () => {
  test('health live/ready abertos', async () => {
    expect((await call('GET', '/health/live')).status).toBe(200);
    expect((await call('GET', '/health/ready')).body).toMatchObject({ status: 'ok' });
  });

  test('X-Correlation-Id é propagado ou gerado', async () => {
    const res = await call('GET', '/health/live', { headers: { 'x-correlation-id': 'abc-123' } });
    expect(res.headers.get('x-correlation-id')).toBe('abc-123');
    expect((await call('GET', '/health/live')).headers.get('x-correlation-id')).toMatch(/^[0-9a-f-]{36}$/);
  });
});
