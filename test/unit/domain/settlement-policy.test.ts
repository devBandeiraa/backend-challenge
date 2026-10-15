import { describe, expect, test } from 'bun:test';
import { FailureCode as F } from '../../../src/domain/wagering/failure-code';
import { SettlementPolicy } from '../../../src/domain/wagering/settlement-policy';
import type { WagerTransaction } from '../../../src/domain/wagering/wager-transaction';
import { WagerTransactionKind as K } from '../../../src/domain/wagering/wager-transaction-kind';
import { LedgerDirection } from '../../../src/domain/wallet/ledger-direction';
import type { Wallet } from '../../../src/domain/wallet/wallet';
import { brl, id, newTx, openWallet, processed, T0, usd } from '../../support/builders';

const policy = new SettlementPolicy();

function decide(wallet: Wallet, tx: WagerTransaction, reference?: WagerTransaction, referenceAlreadyReversed = false) {
  return policy.decide({ transaction: tx, wallet, reference, referenceAlreadyReversed });
}

function processedBet(wallet: Wallet, amount = '30.00') {
  return processed(newTx(wallet, K.Bet, { amount, externalTransactionId: `bet-${id()}` }));
}

describe('BET', () => {
  test('com saldo: aplica débito', () => {
    const w = openWallet('100.00');
    expect(decide(w, newTx(w, K.Bet, { amount: '100.00' }))).toEqual({
      type: 'apply',
      direction: LedgerDirection.Debit,
      referenceTransactionId: undefined,
    });
  });

  test('sem saldo: rejeita INSUFFICIENT_FUNDS', () => {
    const w = openWallet('100.00');
    expect(decide(w, newTx(w, K.Bet, { amount: '100.01' }))).toMatchObject({ type: 'reject', code: F.InsufficientFunds });
  });
});

describe('WIN', () => {
  test('sem referência: aplica crédito', () => {
    const w = openWallet('0.00');
    expect(decide(w, newTx(w, K.Win))).toMatchObject({ type: 'apply', direction: LedgerDirection.Credit });
  });

  test('com referência à BET processada da rodada: aplica crédito com qualquer valor', () => {
    const w = openWallet();
    const bet = processedBet(w, '10.00');
    const win = newTx(w, K.Win, { amount: '55.00', referenceExternalTransactionId: bet.externalTransactionId });
    expect(decide(w, win, bet)).toEqual({ type: 'apply', direction: LedgerDirection.Credit, referenceTransactionId: bet.id });
  });

  test('referência ainda não chegou: aguarda', () => {
    const w = openWallet();
    expect(decide(w, newTx(w, K.Win, { referenceExternalTransactionId: 'bet-late' }))).toEqual({ type: 'await-reference' });
  });

  test('referência a algo que não é BET: INVALID_REFERENCE_KIND', () => {
    const w = openWallet();
    const otherWin = processed(newTx(w, K.Win));
    const win = newTx(w, K.Win, { referenceExternalTransactionId: otherWin.externalTransactionId });
    expect(decide(w, win, otherWin)).toMatchObject({ type: 'reject', code: F.InvalidReferenceKind });
  });
});

describe('LOSS', () => {
  test('sem efeito no saldo (no-effect), mesmo com valor', () => {
    const w = openWallet('0.00');
    expect(decide(w, newTx(w, K.Loss, { amount: '25.00' }))).toEqual({ type: 'no-effect', referenceTransactionId: undefined });
  });

  test('referenciando a BET: no-effect com o id da referência', () => {
    const w = openWallet();
    const bet = processedBet(w);
    const loss = newTx(w, K.Loss, { referenceExternalTransactionId: bet.externalTransactionId });
    expect(decide(w, loss, bet)).toEqual({ type: 'no-effect', referenceTransactionId: bet.id });
  });
});

describe('REFUND', () => {
  test('de BET processada, mesmo valor: crédito', () => {
    const w = openWallet('70.00');
    const bet = processedBet(w);
    const refund = newTx(w, K.Refund, { amount: '30.00', referenceExternalTransactionId: bet.externalTransactionId });
    expect(decide(w, refund, bet)).toEqual({ type: 'apply', direction: LedgerDirection.Credit, referenceTransactionId: bet.id });
  });

  test('só referencia BET', () => {
    const w = openWallet();
    const win = processed(newTx(w, K.Win, { amount: '30.00' }));
    const refund = newTx(w, K.Refund, { amount: '30.00', referenceExternalTransactionId: win.externalTransactionId });
    expect(decide(w, refund, win)).toMatchObject({ type: 'reject', code: F.InvalidReferenceKind });
  });

  test('valor diferente da referência: AMOUNT_MISMATCH (reversão parcial fora de escopo)', () => {
    const w = openWallet();
    const bet = processedBet(w, '30.00');
    const refund = newTx(w, K.Refund, { amount: '29.99', referenceExternalTransactionId: bet.externalTransactionId });
    expect(decide(w, refund, bet)).toMatchObject({ type: 'reject', code: F.AmountMismatch, referenceTransactionId: bet.id });
  });

  test('referência já revertida (qualquer kind, decisão B): REFERENCE_ALREADY_REVERSED', () => {
    const w = openWallet();
    const bet = processedBet(w);
    const refund = newTx(w, K.Refund, { amount: '30.00', referenceExternalTransactionId: bet.externalTransactionId });
    expect(decide(w, refund, bet, true)).toMatchObject({ type: 'reject', code: F.ReferenceAlreadyReversed });
  });

  test('BET rejeitada: REFERENCE_NOT_PROCESSED', () => {
    const w = openWallet();
    const bet = newTx(w, K.Bet, { amount: '30.00' });
    bet.reject(F.InsufficientFunds, T0, { balance: brl('0.00'), walletVersion: 1 });
    const refund = newTx(w, K.Refund, { amount: '30.00', referenceExternalTransactionId: bet.externalTransactionId });
    expect(decide(w, refund, bet)).toMatchObject({ type: 'reject', code: F.ReferenceNotProcessed });
  });

  test('referência ausente: aguarda (PENDING_REFERENCE)', () => {
    const w = openWallet();
    const refund = newTx(w, K.Refund, { referenceExternalTransactionId: 'bet-not-yet' });
    expect(decide(w, refund)).toEqual({ type: 'await-reference' });
  });
});

describe('ROLLBACK', () => {
  test('de BET: crédito (devolve a aposta)', () => {
    const w = openWallet('70.00');
    const bet = processedBet(w);
    const rb = newTx(w, K.Rollback, { amount: '30.00', referenceExternalTransactionId: bet.externalTransactionId });
    expect(decide(w, rb, bet)).toMatchObject({ type: 'apply', direction: LedgerDirection.Credit });
  });

  test('de WIN: débito', () => {
    const w = openWallet('100.00');
    const win = processed(newTx(w, K.Win, { amount: '40.00' }));
    const rb = newTx(w, K.Rollback, { amount: '40.00', referenceExternalTransactionId: win.externalTransactionId });
    expect(decide(w, rb, win)).toMatchObject({ type: 'apply', direction: LedgerDirection.Debit });
  });

  test('de REFUND: débito (ROLLBACK(REFUND) continua permitido na decisão B)', () => {
    const w = openWallet('100.00');
    const refund = processed(newTx(w, K.Refund, { amount: '30.00', referenceExternalTransactionId: 'bet-x' }));
    const rb = newTx(w, K.Rollback, { amount: '30.00', referenceExternalTransactionId: refund.externalTransactionId });
    expect(decide(w, rb, refund)).toMatchObject({ type: 'apply', direction: LedgerDirection.Debit });
  });

  test('débito que estouraria o saldo: REVERSAL_WOULD_OVERDRAW (distinto de INSUFFICIENT_FUNDS)', () => {
    const w = openWallet('10.00');
    const win = processed(newTx(w, K.Win, { amount: '40.00' }));
    const rb = newTx(w, K.Rollback, { amount: '40.00', referenceExternalTransactionId: win.externalTransactionId });
    expect(decide(w, rb, win)).toMatchObject({ type: 'reject', code: F.ReversalWouldOverdraw, referenceTransactionId: win.id });
  });

  test.each([K.Loss, K.Rollback])('não reverte %s: INVALID_REFERENCE_KIND', (refKind) => {
    const w = openWallet();
    const ref =
      refKind === K.Rollback
        ? processed(newTx(w, K.Rollback, { amount: '10.00', referenceExternalTransactionId: 'y' }))
        : processed(newTx(w, K.Loss, { amount: '10.00' }));
    const rb = newTx(w, K.Rollback, { amount: '10.00', referenceExternalTransactionId: ref.externalTransactionId });
    expect(decide(w, rb, ref)).toMatchObject({ type: 'reject', code: F.InvalidReferenceKind });
  });

  test('referência ainda PENDING_REFERENCE (ROLLBACK de REFUND pendente): aguarda junto', () => {
    const w = openWallet();
    const refund = newTx(w, K.Refund, { amount: '10.00', referenceExternalTransactionId: 'bet-late' });
    refund.markPendingReference(T0, T0);
    const rb = newTx(w, K.Rollback, { amount: '10.00', referenceExternalTransactionId: refund.externalTransactionId });
    expect(decide(w, rb, refund)).toEqual({ type: 'await-reference' });
  });
});

describe('contexto da referência: REFERENCE_MISMATCH', () => {
  const w = openWallet('100.00');
  const bet = processedBet(w);
  const cases: [string, Parameters<typeof newTx>[2]][] = [
    ['outra rodada', { roundId: 'round-2' }],
    ['outro provider', { providerId: 'provider-b' }],
  ];
  test.each(cases)('%s', (_label, overrides) => {
    const refund = newTx(w, K.Refund, { amount: '30.00', referenceExternalTransactionId: bet.externalTransactionId, ...overrides });
    // sem referenceTransactionId: a referência pode estar em outra wallet (FK composta)
    expect(decide(w, refund, bet)).toEqual({ type: 'reject', code: F.ReferenceMismatch, referenceTransactionId: undefined });
  });

  test('referência de outra wallet do mesmo player', () => {
    const other = openWallet('100.00', { playerId: w.playerId, currency: 'BRL' });
    const foreignBet = processedBet(other);
    const refund = newTx(w, K.Refund, { amount: '30.00', referenceExternalTransactionId: foreignBet.externalTransactionId });
    expect(decide(w, refund, foreignBet)).toMatchObject({ type: 'reject', code: F.ReferenceMismatch });
  });
});

describe('validações da wallet (antes de qualquer referência)', () => {
  test('player diferente do dono da wallet: PLAYER_MISMATCH', () => {
    const w = openWallet();
    expect(decide(w, newTx(w, K.Bet, { playerId: id() }))).toMatchObject({ type: 'reject', code: F.PlayerMismatch });
  });

  test('conflito de moeda: CURRENCY_MISMATCH como rejeição de negócio', () => {
    const w = openWallet('100.00');
    expect(decide(w, newTx(w, K.Bet, { money: usd('1.00') }))).toMatchObject({ type: 'reject', code: F.CurrencyMismatch });
  });
});
