import { describe, expect, test } from 'bun:test';
import { InvalidEntityError, InvalidTransactionStateError } from '../../../src/domain/shared/domain-error';
import { FailureCode } from '../../../src/domain/wagering/failure-code';
import { WagerTransaction } from '../../../src/domain/wagering/wager-transaction';
import { WagerTransactionKind as K, WagerTransactionStatus as S } from '../../../src/domain/wagering/wager-transaction-kind';
import { LedgerDirection } from '../../../src/domain/wallet/ledger-direction';
import { brl, id, newTx, openWallet, processed, T0 } from '../../support/builders';

const result = { balance: brl('90.00'), walletVersion: 2 };

describe('WagerTransaction.create', () => {
  const wallet = openWallet();

  test('nasce PENDING sem failureCode nem resultado', () => {
    const tx = newTx(wallet, K.Bet);
    expect(tx.status).toBe(S.Pending);
    expect(tx.failureCode).toBeUndefined();
    expect(tx.result).toBeUndefined();
    expect(tx.isTerminal()).toBe(false);
  });

  test('OPENING não pode ser submetido', () => {
    expect(() => newTx(wallet, K.Opening)).toThrow(InvalidEntityError);
  });

  test.each([['internal'], ['Internal'], ['INTERNAL']])('providerId %p é reservado às transações internas', (providerId) => {
    expect(() => newTx(wallet, K.Bet, { providerId })).toThrow(/reserved/);
  });

  test.each([K.Refund, K.Rollback])('%s exige referenceExternalTransactionId', (kind) => {
    expect(() => newTx(wallet, kind)).toThrow(InvalidEntityError);
    expect(newTx(wallet, kind, { referenceExternalTransactionId: 'bet-1' }).requiresReference()).toBe(true);
  });

  test('BET não pode referenciar; WIN e LOSS podem (opcional)', () => {
    expect(() => newTx(wallet, K.Bet, { referenceExternalTransactionId: 'x' })).toThrow(InvalidEntityError);
    expect(newTx(wallet, K.Win, { referenceExternalTransactionId: 'bet-1' }).referenceExternalTransactionId).toBe('bet-1');
    expect(newTx(wallet, K.Win).referenceExternalTransactionId).toBeUndefined();
  });

  test('valor: zero só é aceito em LOSS', () => {
    expect(newTx(wallet, K.Loss, { amount: '0.00' }).money.amount).toBe('0.00');
    for (const kind of [K.Bet, K.Win]) expect(() => newTx(wallet, kind, { amount: '0.00' })).toThrow(InvalidEntityError);
  });

  test.each(['providerId', 'externalTransactionId', 'roundId', 'gameId'] as const)('%s vazio é inválido', (field) => {
    expect(() => newTx(wallet, K.Bet, { [field]: '' })).toThrow(InvalidEntityError);
  });

  test('payloadHash precisa ser SHA-256 hex', () => {
    expect(() => newTx(wallet, K.Bet, { payloadHash: 'abc' })).toThrow(InvalidEntityError);
  });

  test('createOpening já nasce PROCESSED com snapshot do saldo inicial', () => {
    const tx = WagerTransaction.createOpening({
      id: id(),
      walletId: wallet.id,
      playerId: wallet.playerId,
      money: brl('1000.00'),
      payloadHash: 'a'.repeat(64),
      correlationId: 'c',
      at: T0,
    });
    expect(tx.kind).toBe(K.Opening);
    expect(tx.status).toBe(S.Processed);
    expect(tx.result?.balance.amount).toBe('1000.00');
    expect(tx.result?.walletVersion).toBe(1);
    expect(tx.idempotencyKey).toBe(`internal:opening:${wallet.id}`);
  });
});

describe('WagerTransaction: transições', () => {
  const wallet = openWallet();

  test('PENDING → PROCESSED grava referência, processedAt e snapshot', () => {
    const tx = newTx(wallet, K.Bet);
    tx.markProcessed(undefined, T0, result);
    expect(tx.status).toBe(S.Processed);
    expect(tx.processedAt).toEqual(T0);
    expect(tx.result).toEqual(result);
  });

  test('reversão não pode ser PROCESSED sem referência resolvida', () => {
    const tx = newTx(wallet, K.Refund, { referenceExternalTransactionId: 'bet-1' });
    expect(() => tx.markProcessed(undefined, T0, result)).toThrow(InvalidTransactionStateError);
  });

  test('PENDING → PENDING_REFERENCE → PENDING_REFERENCE → PROCESSED, contando tentativas', () => {
    const tx = newTx(wallet, K.Refund, { referenceExternalTransactionId: 'bet-1' });
    tx.markPendingReference(new Date(T0.getTime() + 500), T0);
    expect(tx.status).toBe(S.PendingReference);
    expect(tx.attempts).toBe(1);
    tx.markPendingReference(new Date(T0.getTime() + 1500), T0);
    expect(tx.attempts).toBe(2);
    tx.markProcessed(id(), T0, result);
    expect(tx.status).toBe(S.Processed);
    expect(tx.nextAttemptAt).toBeUndefined();
  });

  test('reject grava failureCode e snapshot; fail grava código sem snapshot', () => {
    const a = newTx(wallet, K.Bet);
    a.reject(FailureCode.InsufficientFunds, T0, result);
    expect([a.status, a.failureCode]).toEqual([S.Rejected, 'INSUFFICIENT_FUNDS']);
    const b = newTx(wallet, K.Bet);
    b.fail(FailureCode.ProcessingFailed, T0);
    expect([b.status, b.failureCode, b.result]).toEqual([S.Failed, 'PROCESSING_FAILED', undefined]);
  });

  test.each([
    ['PROCESSED', (tx: WagerTransaction) => tx.markProcessed(undefined, T0, result)],
    ['REJECTED', (tx: WagerTransaction) => tx.reject(FailureCode.InsufficientFunds, T0, result)],
    ['FAILED', (tx: WagerTransaction) => tx.fail(FailureCode.ProcessingFailed, T0)],
  ])('estado terminal %s não aceita nenhuma transição', (_s, toTerminal) => {
    const tx = newTx(wallet, K.Bet);
    toTerminal(tx);
    expect(tx.isTerminal()).toBe(true);
    expect(() => tx.markProcessed(undefined, T0, result)).toThrow(InvalidTransactionStateError);
    expect(() => tx.markPendingReference(T0, T0)).toThrow(InvalidTransactionStateError);
    expect(() => tx.reject(FailureCode.InsufficientFunds, T0, result)).toThrow(InvalidTransactionStateError);
    expect(() => tx.fail(FailureCode.ProcessingFailed, T0)).toThrow(InvalidTransactionStateError);
  });

  test('rehydrate não revalida: reconstrói estado terminal persistido', () => {
    const original = processed(newTx(wallet, K.Bet));
    const copy = WagerTransaction.rehydrate({
      ...original,
      roundId: original.roundId,
      gameId: original.gameId,
      updatedAt: original.updatedAt,
      status: original.status,
      referenceTransactionId: original.referenceTransactionId,
      failureCode: original.failureCode,
      processedAt: original.processedAt,
      result: original.result,
      attempts: original.attempts,
      nextAttemptAt: original.nextAttemptAt,
    });
    expect(copy.status).toBe(S.Processed);
  });
});

describe('WagerTransaction: consultas', () => {
  const wallet = openWallet();

  test('affectsBalance é false só para LOSS', () => {
    expect(newTx(wallet, K.Loss).affectsBalance()).toBe(false);
    expect(newTx(wallet, K.Bet).affectsBalance()).toBe(true);
  });

  test('matchesPayload compara o hash', () => {
    const tx = newTx(wallet, K.Bet);
    expect(tx.matchesPayload(tx.payloadHash)).toBe(true);
    expect(tx.matchesPayload('b'.repeat(64))).toBe(false);
  });

  test('direções: BET débito; WIN/REFUND crédito; ROLLBACK inverte a referência; LOSS não tem', () => {
    const bet = newTx(wallet, K.Bet);
    const win = newTx(wallet, K.Win);
    const refund = newTx(wallet, K.Refund, { referenceExternalTransactionId: bet.externalTransactionId });
    const rollback = newTx(wallet, K.Rollback, { referenceExternalTransactionId: 'x' });
    expect(bet.ledgerDirectionFor()).toBe(LedgerDirection.Debit);
    expect(win.ledgerDirectionFor()).toBe(LedgerDirection.Credit);
    expect(refund.ledgerDirectionFor(bet)).toBe(LedgerDirection.Credit);
    expect(rollback.ledgerDirectionFor(bet)).toBe(LedgerDirection.Credit);
    expect(rollback.ledgerDirectionFor(win)).toBe(LedgerDirection.Debit);
    expect(rollback.ledgerDirectionFor(refund)).toBe(LedgerDirection.Debit);
    expect(() => rollback.ledgerDirectionFor()).toThrow(InvalidTransactionStateError);
    expect(() => newTx(wallet, K.Loss).ledgerDirectionFor()).toThrow(InvalidTransactionStateError);
  });
});
