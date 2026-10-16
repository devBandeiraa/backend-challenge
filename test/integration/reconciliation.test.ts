/**
 * Reconciliação: compara saldo materializado com o ledger, NÃO corrige; loga, mede e sinaliza.
 * A divergência é forçada de verdade no Postgres, desligando os triggers da sessão com
 * session_replication_role = replica (inclusive a verificação deferred wallet↔ledger), como faria
 * uma escrita manual/externa contornando a aplicação.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import { walletBalance } from '../support/app-context';
import { createOwnerOrm, createTestOrm } from '../support/db';
import { http, parseLogLines, startHttpApp } from '../support/http-app';
import { ledgerInvariantAfterEach } from '../support/invariants';

let baseUrl: string;
/** Invariante saldo × ledger no afterEach, para toda wallet registrada com touch() no teste corrente. */
const touch = ledgerInvariantAfterEach(() => orm);
let close: () => Promise<void>;
let orm: MikroORM;
/** Conexão do OWNER: só ele (superuser no compose de dev) consegue contornar triggers; a app não. */
let ownerOrm: MikroORM;
const logLines: string[] = [];

beforeAll(async () => {
  ({ baseUrl, close } = await startHttpApp({ logLines }));
  orm = await createTestOrm(3);
  ownerOrm = await createOwnerOrm(2);
});
afterAll(async () => {
  await close();
  await orm.close(true);
  await ownerOrm.close(true);
});

async function wallet(amount: string) {
  const res = await http(baseUrl, 'POST', '/wallets', { body: { playerId: crypto.randomUUID(), initialBalance: { amount, currency: 'BRL' } } });
  return res.body as { id: string; playerId: string };
}

async function bet(w: { id: string; playerId: string }, amount: string) {
  const ext = `rc-${crypto.randomUUID()}`;
  await http(baseUrl, 'POST', '/wagering/transactions', {
    headers: { 'idempotency-key': `provider-a:${ext}` },
    body: { providerId: 'provider-a', externalTransactionId: ext, playerId: w.playerId, walletId: w.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount, currency: 'BRL' } },
  });
}

// Invariante: verificado ao fim do teste consistente; as wallets corrompidas abaixo divergem DE PROPÓSITO.
/** Escrita que contorna a aplicação E os triggers: exige a conexão do owner (superuser no compose de dev). */
async function corrupt(sql: string, params: unknown[]) {
  await ownerOrm.em.fork().transactional(async (em) => {
    await em.execute(`SET LOCAL session_replication_role = replica`);
    await em.execute(sql, params);
  });
}

const mismatchMetric = async () => {
  const text = (await http<string>(baseUrl, 'GET', '/metrics')).body;
  const line = text.split('\n').find((l) => l.startsWith('wagering_reconciliation_mismatches_total '));
  return Number(line?.split(' ')[1] ?? 0);
};

describe('POST /wallets/:id/reconciliation', () => {
  test('wallet íntegra: consistent true, contrato do enunciado', async () => {
    const w = await wallet('1000.00');
    await bet(w, '25.00');
    const res = await http(baseUrl, 'POST', `/wallets/${w.id}/reconciliation`);
    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      walletId: w.id,
      storedBalance: { amount: '975.00', currency: 'BRL' },
      calculatedBalance: { amount: '975.00', currency: 'BRL' },
      difference: { amount: '0.00', currency: 'BRL' },
      consistent: true,
      checkedEntries: 2,
      walletVersion: 2,
      ledgerVersion: 2,
    });
    touch(w.id); // invariante conferida no afterEach
  });

  test('divergência forçada no saldo: consistent false, diferença exata, log de erro, métrica; NADA é corrigido', async () => {
    const w = await wallet('100.00');
    await corrupt(`UPDATE wallets SET balance = balance + 5 WHERE id = ?`, [w.id]);
    const before = await mismatchMetric();
    logLines.length = 0;

    const res = await http(baseUrl, 'POST', `/wallets/${w.id}/reconciliation`, { headers: { 'x-correlation-id': 'recon-1' } });

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      storedBalance: { amount: '105.00' },
      calculatedBalance: { amount: '100.00' },
      difference: { amount: '5.00', currency: 'BRL' },
      consistent: false,
      checkedEntries: 1,
    });
    expect((await mismatchMetric()) - before).toBe(1);
    const log = parseLogLines(logLines).find((l) => l.msg === 'reconciliation mismatch');
    expect(log).toMatchObject({ level: 'error', walletId: w.id, difference: '5.00', correlationId: 'recon-1' });
    expect(await walletBalance(orm, w.id)).toBe('105.00'); // sinalizado, não "consertado"
  });

  test('divergência só de versão (saldo igual): também inconsistente', async () => {
    const w = await wallet('10.00');
    await corrupt(`UPDATE wallets SET version = version + 3 WHERE id = ?`, [w.id]);
    const res = await http(baseUrl, 'POST', `/wallets/${w.id}/reconciliation`);
    expect(res.body).toMatchObject({ difference: { amount: '0.00' }, consistent: false, walletVersion: 4, ledgerVersion: 1 });
  });

  test('saldo abaixo do ledger: diferença negativa', async () => {
    const w = await wallet('50.00');
    await corrupt(`UPDATE wallets SET balance = 20 WHERE id = ?`, [w.id]);
    const res = await http(baseUrl, 'POST', `/wallets/${w.id}/reconciliation`);
    expect(res.body).toMatchObject({ difference: { amount: '-30.00' }, consistent: false });
  });

  test('404 para wallet inexistente, 400 para id inválido', async () => {
    expect((await http(baseUrl, 'POST', `/wallets/${crypto.randomUUID()}/reconciliation`)).status).toBe(404);
    expect((await http(baseUrl, 'POST', `/wallets/nope/reconciliation`)).status).toBe(400);
  });
});
