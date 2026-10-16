import { describe, expect, test } from 'bun:test';
import { ConnectionException, DriverException, UniqueConstraintViolationException } from '@mikro-orm/core';
import { TransientInfrastructureError } from '../../../src/application/errors';
import type { TransactionScope, UnitOfWork } from '../../../src/application/ports';
import { ResolvePendingReferencesUseCase } from '../../../src/application/wagering/resolve-pending-references';
import { classifyPgError, isTransientDbError } from '../../../src/infrastructure/persistence/pg-errors';
import { SystemClock, UuidV7Generator } from '../../../src/infrastructure/system';

const pgError = (code: string, message = 'server error', extra: Record<string, unknown> = {}) =>
  Object.assign(new Error(message), { code, ...extra });

describe('classificação de erros do banco', () => {
  test.each([
    ['DriverException "not queryable" (sem SQLSTATE)', new DriverException(new Error('Client has encountered a connection error and is not queryable'))],
    ['Error "Connection terminated unexpectedly"', new Error('Connection terminated unexpectedly')],
    ['pg-pool: timeout de aquisição', new Error('timeout exceeded when trying to connect')],
    ['watchdog de query', new Error('query exceeded 18000 ms without a response: connection destroyed')],
    ['DriverException sem SQLSTATE e mensagem desconhecida', new DriverException(new Error('something odd in the socket'))],
    ['ConnectionException', new ConnectionException(new Error('boom'))],
    ['57P01 admin_shutdown', pgError('57P01', 'terminating connection due to administrator command')],
    ['classe 08 (08006)', pgError('08006', 'connection failure')],
    ['ECONNREFUSED', pgError('ECONNREFUSED', 'connect ECONNREFUSED 172.0.0.3:5432')],
    ['erro de conexão como causa encadeada', Object.assign(new Error('wrapper'), { cause: new Error('Connection terminated') })],
    ['DriverException com previous de conexão', Object.assign(new Error('outer'), { previous: pgError('57P01') })],
  ])('%s → unavailable', (_label, error) => {
    expect(classifyPgError(error)).toBe('unavailable');
    expect(isTransientDbError(error)).toBe(true);
  });

  test.each([
    ['55P03 lock_not_available', pgError('55P03')],
    ['40P01 deadlock', pgError('40P01')],
    ['23505 em wt_single_reversal_uq', pgError('23505', 'dup', { constraint: 'wt_single_reversal_uq' })],
  ])('%s → retryable', (_label, error) => {
    expect(classifyPgError(error)).toBe('retryable');
  });

  test.each([
    ['TypeError (bug)', new TypeError('cannot read properties of undefined')],
    ['23505 de outra constraint', new UniqueConstraintViolationException(pgError('23505', 'dup', { constraint: 'wallets_player_currency_uq' }))],
    ['WL004 (trigger de integridade)', pgError('WL004', 'wallet diverges from ledger')],
    ['DriverException COM SQLSTATE de servidor', new DriverException(pgError('42601', 'syntax error'))],
  ])('%s → other (não transitório)', (_label, error) => {
    expect(classifyPgError(error)).toBe('other');
    expect(isTransientDbError(error)).toBe(false);
  });
});

describe('worker de PENDING_REFERENCE nunca marca FAILED por erro de conexão', () => {
  /** UoW que reivindica uma linha e falha com o erro dado; conta quantas transações foram abertas. */
  function failingUow(error: unknown) {
    let runs = 0;
    const tx = { id: 'tx-1', walletId: 'w-1', status: 'PENDING_REFERENCE' };
    const uow: UnitOfWork = {
      run: async (work) => {
        runs++;
        const scope = {
          transactions: { claimDuePendingReference: async () => tx, findById: async () => tx, save: async () => undefined },
          wallets: { lockById: async () => { throw error; } },
        } as unknown as TransactionScope;
        return work(scope);
      },
    };
    return { uow, runs: () => runs };
  }

  test.each([
    ['DriverException "not queryable" cru (sem passar pela unit of work)', new DriverException(new Error('Client has encountered a connection error and is not queryable'))],
    ['TransientInfrastructureError', new TransientInfrastructureError('database unavailable')],
    ['57P01', pgError('57P01')],
  ])('%s: o erro sobe, a linha continua pendente (nenhuma 2ª transação de FAILED)', async (_label, error) => {
    const { uow, runs } = failingUow(error);
    const useCase = new ResolvePendingReferencesUseCase(uow, new SystemClock(), new UuidV7Generator(), undefined, isTransientDbError);
    await expect(useCase.runOnce(1)).rejects.toBe(error);
    expect(runs()).toBe(1);
  });

  test('erro de programação (TypeError) continua virando FAILED (linha venenosa)', async () => {
    const { uow, runs } = failingUow(new TypeError('bug'));
    const useCase = new ResolvePendingReferencesUseCase(uow, new SystemClock(), new UuidV7Generator(), undefined, isTransientDbError);
    await useCase.runOnce(1).catch(() => undefined);
    expect(runs()).toBe(2); // a 2ª transação é a gravação do FAILED
  });
});
