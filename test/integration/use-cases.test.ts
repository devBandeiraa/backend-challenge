import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { IdempotencyConflictError, UnknownWalletError, ValidationError, WalletAlreadyExistsError } from '../../src/application/errors';
import { type AppContext, assertLedgerInvariant, countRows, createAppContext, wagerCommand } from '../support/app-context';

let app: AppContext;
const touched: string[] = [];

beforeAll(async () => {
  app = await createAppContext();
});
afterAll(async () => {
  for (const id of touched) await assertLedgerInvariant(app.orm, id);
  await app.close();
});

async function wallet(amount = '100.00', currency = 'BRL') {
  const w = await app.openWallet.execute({
    playerId: crypto.randomUUID(),
    initialBalance: { amount, currency },
    correlationId: 'test',
  });
  touched.push(w.id);
  return w;
}

describe('OpenWallet', () => {
  test('saldo inicial > 0: wallet v1 + OPENING PROCESSED + CREDIT no ledger + 2 eventos, mesma transação', async () => {
    const w = await wallet('1000.00');
    expect(w).toMatchObject({ balance: { amount: '1000.00', currency: 'BRL' }, version: 1 });
    expect(await countRows(app.orm, `wager_transactions WHERE wallet_id = ? AND kind = 'OPENING' AND status = 'PROCESSED'`, [w.id])).toBe(1);
    expect(await countRows(app.orm, `wallet_ledger_entries WHERE wallet_id = ? AND direction = 'CREDIT' AND wallet_version = 1`, [w.id])).toBe(1);
    expect(await countRows(app.orm, `outbox_messages WHERE correlation_id = 'test' AND payload->'data'->>'walletId' = ?`, [w.id])).toBe(2);
    await assertLedgerInvariant(app.orm, w.id);
  });

  test('saldo inicial zero: sem OPENING e sem ledger', async () => {
    const w = await wallet('0.00');
    expect(await countRows(app.orm, `wager_transactions WHERE wallet_id = ?`, [w.id])).toBe(0);
    expect(await countRows(app.orm, `wallet_ledger_entries WHERE wallet_id = ?`, [w.id])).toBe(0);
  });

  test('mesmo player + moeda: WalletAlreadyExistsError; outra moeda é permitida', async () => {
    const w = await wallet('1.00');
    await expect(
      app.openWallet.execute({ playerId: w.playerId, initialBalance: { amount: '5.00', currency: 'BRL' }, correlationId: 't' }),
    ).rejects.toBeInstanceOf(WalletAlreadyExistsError);
    const usd = await app.openWallet.execute({ playerId: w.playerId, initialBalance: { amount: '5.00', currency: 'USD' }, correlationId: 't' });
    touched.push(usd.id);
    expect(usd.balance.currency).toBe('USD');
  });

  test('valor inválido é ValidationError', async () => {
    await expect(
      app.openWallet.execute({ playerId: crypto.randomUUID(), initialBalance: { amount: '1e3', currency: 'BRL' }, correlationId: 't' }),
    ).rejects.toBeInstanceOf(ValidationError);
  });
});

describe('ProcessWagerTransaction', () => {
  test('BET debita, version 2, um lançamento DEBIT', async () => {
    const w = await wallet('1000.00');
    const out = await app.processTransaction.execute(wagerCommand(w, { money: { amount: '25.00', currency: 'BRL' } }));
    expect(out).toMatchObject({ status: 'PROCESSED', balance: { amount: '975.00', currency: 'BRL' }, idempotentReplay: false });
    expect(await countRows(app.orm, `wallet_ledger_entries WHERE transaction_id = ? AND direction = 'DEBIT'`, [out.transactionId])).toBe(1);
    expect(await countRows(app.orm, `wallets WHERE id = ? AND version = 2`, [w.id])).toBe(1);
  });

  test('replay idêntico devolve o resultado ORIGINAL (saldo daquele momento), sem novo efeito', async () => {
    const w = await wallet('100.00');
    const cmd = wagerCommand(w, { money: { amount: '30.00', currency: 'BRL' } });
    const first = await app.processTransaction.execute(cmd);
    await app.processTransaction.execute(wagerCommand(w, { money: { amount: '5.00', currency: 'BRL' } })); // saldo muda depois
    // correlationId é metadado de transporte, fora do hash: continua sendo replay
    const replay = await app.processTransaction.execute({ ...cmd, correlationId: 'other' });
    expect(replay).toEqual({ ...first, idempotentReplay: true });
    expect(replay.balance?.amount).toBe('70.00');
    expect(await countRows(app.orm, `wallet_ledger_entries WHERE transaction_id = ?`, [first.transactionId])).toBe(1);
  });

  test('mesma key com payload diferente: 409 IDEMPOTENCY_KEY_PAYLOAD_MISMATCH, sem efeito', async () => {
    const w = await wallet('100.00');
    const cmd = wagerCommand(w);
    await app.processTransaction.execute(cmd);
    const err = await app.processTransaction.execute({ ...cmd, money: { amount: '11.00', currency: 'BRL' } }).catch((e) => e);
    expect(err).toBeInstanceOf(IdempotencyConflictError);
    expect(err.code).toBe('IDEMPOTENCY_KEY_PAYLOAD_MISMATCH');
    expect(await countRows(app.orm, `wallets WHERE id = ? AND balance = 90`, [w.id])).toBe(1);
  });

  test('mesmo provider+externalId com outra key: 409 EXTERNAL_TRANSACTION_ID_CONFLICT', async () => {
    const w = await wallet('100.00');
    const cmd = wagerCommand(w);
    await app.processTransaction.execute(cmd);
    const err = await app.processTransaction.execute({ ...cmd, idempotencyKey: 'another-key' }).catch((e) => e);
    expect(err).toBeInstanceOf(IdempotencyConflictError);
    expect(err.code).toBe('EXTERNAL_TRANSACTION_ID_CONFLICT');
  });

  test('BET sem saldo: REJECTED INSUFFICIENT_FUNDS persistido, sem ledger, version inalterada; replay idem', async () => {
    const w = await wallet('10.00');
    const cmd = wagerCommand(w, { money: { amount: '10.01', currency: 'BRL' } });
    const out = await app.processTransaction.execute(cmd);
    expect(out).toMatchObject({ status: 'REJECTED', failureCode: 'INSUFFICIENT_FUNDS', balance: { amount: '10.00' } });
    expect(await countRows(app.orm, `wallet_ledger_entries WHERE transaction_id = ?`, [out.transactionId])).toBe(0);
    expect(await countRows(app.orm, `wallets WHERE id = ? AND version = 1`, [w.id])).toBe(1);
    expect(await app.processTransaction.execute(cmd)).toEqual({ ...out, idempotentReplay: true });
  });

  test('LOSS: PROCESSED sem ledger, sem WalletBalanceChanged e sem mudar version', async () => {
    const w = await wallet('50.00');
    const out = await app.processTransaction.execute(wagerCommand(w, { kind: 'LOSS', money: { amount: '0.00', currency: 'BRL' } }));
    expect(out).toMatchObject({ status: 'PROCESSED', balance: { amount: '50.00' } });
    expect(await countRows(app.orm, `wallet_ledger_entries WHERE transaction_id = ?`, [out.transactionId])).toBe(0);
    expect(await countRows(app.orm, `outbox_messages WHERE event_type = 'WalletBalanceChanged' AND payload->'data'->>'transactionId' = ?`, [out.transactionId])).toBe(0);
    expect(await countRows(app.orm, `outbox_messages WHERE event_type = 'WagerTransactionProcessed' AND aggregate_id = ?`, [out.transactionId])).toBe(1);
  });

  test('ciclo BET → REFUND → ROLLBACK(REFUND), e segunda reversão da BET bloqueada (decisão B)', async () => {
    const w = await wallet('100.00');
    const bet = wagerCommand(w, { externalTransactionId: `bet-${crypto.randomUUID()}`, money: { amount: '40.00', currency: 'BRL' } });
    await app.processTransaction.execute(bet);
    const refund = wagerCommand(w, { kind: 'REFUND', money: { amount: '40.00', currency: 'BRL' }, referenceExternalTransactionId: bet.externalTransactionId, externalTransactionId: `refund-${crypto.randomUUID()}` });
    expect(await app.processTransaction.execute(refund)).toMatchObject({ status: 'PROCESSED', balance: { amount: '100.00' } });

    const rollbackBet = wagerCommand(w, { kind: 'ROLLBACK', money: { amount: '40.00', currency: 'BRL' }, referenceExternalTransactionId: bet.externalTransactionId });
    expect(await app.processTransaction.execute(rollbackBet)).toMatchObject({ status: 'REJECTED', failureCode: 'REFERENCE_ALREADY_REVERSED', balance: { amount: '100.00' } });

    const rollbackRefund = wagerCommand(w, { kind: 'ROLLBACK', money: { amount: '40.00', currency: 'BRL' }, referenceExternalTransactionId: refund.externalTransactionId });
    expect(await app.processTransaction.execute(rollbackRefund)).toMatchObject({ status: 'PROCESSED', balance: { amount: '60.00' } });
  });

  test('ROLLBACK de WIN que estouraria o saldo: REVERSAL_WOULD_OVERDRAW', async () => {
    const w = await wallet('0.00');
    const win = wagerCommand(w, { kind: 'WIN', money: { amount: '50.00', currency: 'BRL' } });
    await app.processTransaction.execute(win);
    await app.processTransaction.execute(wagerCommand(w, { money: { amount: '30.00', currency: 'BRL' } }));
    const rb = wagerCommand(w, { kind: 'ROLLBACK', money: { amount: '50.00', currency: 'BRL' }, referenceExternalTransactionId: win.externalTransactionId });
    expect(await app.processTransaction.execute(rb)).toMatchObject({ status: 'REJECTED', failureCode: 'REVERSAL_WOULD_OVERDRAW', balance: { amount: '20.00' } });
  });

  test('REFUND antes da BET: PENDING_REFERENCE (sem saldo), evento pendente, tentativa agendada', async () => {
    const w = await wallet('100.00');
    const refund = wagerCommand(w, { kind: 'REFUND', referenceExternalTransactionId: 'bet-from-the-future' });
    const out = await app.processTransaction.execute(refund);
    expect(out).toMatchObject({ status: 'PENDING_REFERENCE', balance: undefined, failureCode: undefined });
    expect(await countRows(app.orm, `wager_transactions WHERE id = ? AND attempts = 1 AND next_attempt_at > now()`, [out.transactionId])).toBe(1);
    expect(await countRows(app.orm, `outbox_messages WHERE event_type = 'WagerTransactionPendingReference' AND aggregate_id = ?`, [out.transactionId])).toBe(1);
    expect(await app.processTransaction.execute(refund)).toMatchObject({ status: 'PENDING_REFERENCE', idempotentReplay: true });
  });

  test('moeda diferente da wallet: REJECTED CURRENCY_MISMATCH (saldo na moeda da wallet)', async () => {
    const w = await wallet('100.00');
    const out = await app.processTransaction.execute(wagerCommand(w, { money: { amount: '1.00', currency: 'USD' } }));
    expect(out).toMatchObject({ status: 'REJECTED', failureCode: 'CURRENCY_MISMATCH', balance: { amount: '100.00', currency: 'BRL' } });
  });

  test('player que não é dono: PLAYER_MISMATCH', async () => {
    const w = await wallet('100.00');
    const out = await app.processTransaction.execute(wagerCommand({ id: w.id, playerId: crypto.randomUUID() }));
    expect(out).toMatchObject({ status: 'REJECTED', failureCode: 'PLAYER_MISMATCH' });
  });

  test('wallet inexistente: UnknownWalletError, nada persistido (decisão D)', async () => {
    const cmd = wagerCommand({ id: crypto.randomUUID(), playerId: crypto.randomUUID() });
    await expect(app.processTransaction.execute(cmd)).rejects.toBeInstanceOf(UnknownWalletError);
    expect(await countRows(app.orm, `wager_transactions WHERE idempotency_key = ?`, [cmd.idempotencyKey])).toBe(0);
  });
});
