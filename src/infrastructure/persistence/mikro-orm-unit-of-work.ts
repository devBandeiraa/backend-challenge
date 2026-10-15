import { IsolationLevel } from '@mikro-orm/core';
import type { MikroORM } from '@mikro-orm/postgresql';
import { ApplicationError, TransientInfrastructureError } from '../../application/errors';
import type { TransactionScope, UnitOfWork } from '../../application/ports';
import { ExponentialBackoff } from '../../domain/messaging/backoff-policy';
import { classifyPgError, constraintOf, sqlStateOf } from './pg-errors';
import { createTransactionScope } from './repositories';

export interface UnitOfWorkObserver {
  /** Chamado a CADA retry por conflito de concorrência (vira a métrica de lock conflicts). */
  onRetry?(info: { attempt: number; sqlState: string | undefined; constraint: string | undefined }): void;
  onExhausted?(info: { attempts: number; sqlState: string | undefined }): void;
}

export interface UnitOfWorkOptions {
  maxAttempts?: number;
  /**
   * Teto de espera por lock dentro de cada transação (SET LOCAL lock_timeout). Sem ele, uma
   * hot wallet sob carga extrema empilha conexões esperando indefinidamente; com ele, a espera
   * vira 55P03 → retry com backoff → 503/retry SQS: degradação explícita e mensurável.
   */
  lockTimeoutMs?: number;
  /**
   * Teto de cada instrução (SET LOCAL statement_timeout). Maior que lock_timeout: espera de lock
   * termina em 55P03 (refeita); só uma instrução lenta de verdade termina em 57014, tratada como
   * indisponibilidade (503 / retry SQS) e não refeita aqui, para não multiplicar o pior caso.
   */
  statementTimeoutMs?: number;
}

/**
 * READ COMMITTED + lock pessimista por wallet: cada comando vê o estado confirmado mais recente,
 * e a serialização vem do FOR NO KEY UPDATE, não do nível de isolamento. SERIALIZABLE geraria
 * aborts (40001) em todo conflito de hot wallet, trocando espera curta por retry caro.
 */
export class MikroOrmUnitOfWork implements UnitOfWork {
  private readonly backoff = new ExponentialBackoff({ baseMs: 10, factor: 2, maxMs: 200, jitterRatio: 0.5 });
  private readonly maxAttempts: number;
  private readonly lockTimeoutMs: number;
  private readonly statementTimeoutMs: number;

  constructor(
    private readonly orm: MikroORM,
    options: UnitOfWorkOptions = {},
    private readonly observer: UnitOfWorkObserver = {},
  ) {
    this.maxAttempts = options.maxAttempts ?? 5;
    this.lockTimeoutMs = Math.max(1, Math.floor(options.lockTimeoutMs ?? 5_000));
    this.statementTimeoutMs = Math.max(this.lockTimeoutMs + 1, Math.floor(options.statementTimeoutMs ?? 8_000));
  }

  async run<T>(work: (scope: TransactionScope) => Promise<T>): Promise<T> {
    for (let attempt = 1; ; attempt++) {
      // fork por tentativa: nenhum estado (identity map, entidades) vaza de uma tentativa abortada.
      const em = this.orm.em.fork({ clear: true });
      try {
        return await em.transactional(
          async (tem) => {
            await tem.execute(`SET LOCAL lock_timeout = ${this.lockTimeoutMs}`);
            await tem.execute(`SET LOCAL statement_timeout = ${this.statementTimeoutMs}`);
            return work(createTransactionScope(tem));
          },
          { isolationLevel: IsolationLevel.READ_COMMITTED },
        );
      } catch (error) {
        if (error instanceof ApplicationError) throw error;
        const kind = classifyPgError(error);
        if (kind === 'retryable' && attempt < this.maxAttempts) {
          this.observer.onRetry?.({ attempt, sqlState: sqlStateOf(error), constraint: constraintOf(error) });
          await Bun.sleep(this.backoff.delayMs(attempt));
          continue;
        }
        if (kind === 'retryable') {
          this.observer.onExhausted?.({ attempts: attempt, sqlState: sqlStateOf(error) });
          throw new TransientInfrastructureError(`gave up after ${attempt} attempts (${sqlStateOf(error)})`, error);
        }
        if (kind === 'unavailable') throw new TransientInfrastructureError('database unavailable', error);
        throw error;
      }
    }
  }
}
