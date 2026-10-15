import { IsolationLevel } from '@mikro-orm/core';
import type { MikroORM } from '@mikro-orm/postgresql';
import { ApplicationError, TransientInfrastructureError } from '../../application/errors';
import type { TransactionScope, UnitOfWork } from '../../application/ports';
import { ExponentialBackoff } from '../../domain/messaging/backoff-policy';
import { classifyPgError, constraintOf, sqlStateOf } from './pg-errors';
import { createTransactionScope } from './repositories';

export interface UnitOfWorkObserver {
  onRetry?(info: { attempt: number; sqlState: string | undefined; constraint: string | undefined }): void;
}

/**
 * READ COMMITTED + lock pessimista por wallet: cada comando vê o estado confirmado mais recente,
 * e a serialização vem do FOR NO KEY UPDATE, não do nível de isolamento. SERIALIZABLE geraria
 * aborts (40001) em todo conflito de hot wallet, trocando espera curta por retry caro.
 */
export class MikroOrmUnitOfWork implements UnitOfWork {
  private readonly backoff = new ExponentialBackoff({ baseMs: 10, factor: 2, maxMs: 200, jitterRatio: 0.5 });

  constructor(
    private readonly orm: MikroORM,
    private readonly maxAttempts = 5,
    private readonly observer: UnitOfWorkObserver = {},
  ) {}

  async run<T>(work: (scope: TransactionScope) => Promise<T>): Promise<T> {
    for (let attempt = 1; ; attempt++) {
      // fork por tentativa: nenhum estado (identity map, entidades) vaza de uma tentativa abortada.
      const em = this.orm.em.fork({ clear: true });
      try {
        return await em.transactional((tem) => work(createTransactionScope(tem)), {
          isolationLevel: IsolationLevel.READ_COMMITTED,
        });
      } catch (error) {
        if (error instanceof ApplicationError) throw error;
        const kind = classifyPgError(error);
        if (kind === 'retryable' && attempt < this.maxAttempts) {
          this.observer.onRetry?.({ attempt, sqlState: sqlStateOf(error), constraint: constraintOf(error) });
          await Bun.sleep(this.backoff.delayMs(attempt));
          continue;
        }
        if (kind === 'retryable') {
          throw new TransientInfrastructureError(`gave up after ${attempt} attempts (${sqlStateOf(error)})`, error);
        }
        if (kind === 'unavailable') throw new TransientInfrastructureError('database unavailable', error);
        throw error;
      }
    }
  }
}
