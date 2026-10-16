import { FailureCode } from '../../domain/wagering/failure-code';
import { WagerTransactionStatus } from '../../domain/wagering/wager-transaction-kind';
import { ApplicationError, TransientInfrastructureError } from '../errors';
import type { Clock, IdGenerator, UnitOfWork } from '../ports';
import { DEFAULT_PENDING_REFERENCE_POLICY, type PendingReferencePolicy, TransactionSettler } from './settle-transaction';

export interface ResolveResult {
  transactionId: string;
  status: string;
}

/**
 * Reprocessa transações PENDING_REFERENCE vencidas. Cada uma numa transação SQL própria:
 *   1. reivindica UMA linha vencida com FOR UPDATE SKIP LOCKED: N instâncias dividem o
 *      trabalho sem coordenação e nenhuma processa a mesma linha;
 *   2. trava a wallet (FOR NO KEY UPDATE), como no caminho de submissão;
 *   3. aplica o MESMO settler da submissão (regras, ledger, outbox idênticos).
 * Uma linha por transação mantém o lock da wallet curto e isola falhas.
 *
 * Falhas: transitória → a linha continua pendente (rollback) e volta no próximo ciclo.
 * Não transitória (bug, dado inesperado) → FAILED/PROCESSING_FAILED numa transação separada,
 * para uma linha "venenosa" não ser reivindicada em loop para sempre.
 * Erro de conexão/disponibilidade NUNCA vira FAILED: além de a unit of work já o converter em
 * TransientInfrastructureError, `isTransient` (classificação da infraestrutura) é conferido de novo aqui,
 * porque marcar FAILED por um soluço do banco tornaria terminal uma transação legítima.
 */
export class ResolvePendingReferencesUseCase {
  private readonly settler: TransactionSettler;

  constructor(
    private readonly uow: UnitOfWork,
    private readonly clock: Clock,
    ids: IdGenerator,
    policy: PendingReferencePolicy = DEFAULT_PENDING_REFERENCE_POLICY,
    private readonly isTransient: (error: unknown) => boolean = () => false,
  ) {
    this.settler = new TransactionSettler(clock, ids, policy);
  }

  /** Processa até `limit` transações vencidas. */
  async runOnce(limit: number): Promise<ResolveResult[]> {
    const results: ResolveResult[] = [];
    for (let i = 0; i < limit; i++) {
      let claimedId: string | undefined;
      let result: ResolveResult | undefined;
      try {
        result = await this.uow.run(async (scope) => {
          const tx = await scope.transactions.claimDuePendingReference(this.clock.now());
          if (!tx) return undefined;
          claimedId = tx.id;
          const wallet = await scope.wallets.lockById(tx.walletId);
          if (!wallet) throw new Error(`wallet ${tx.walletId} vanished`); // impossível: sem DELETE
          await this.settler.settle(scope, tx, wallet, { correlationId: tx.correlationId, causationId: tx.id });
          return { transactionId: tx.id, status: tx.status };
        });
      } catch (error) {
        const permanent =
          claimedId !== undefined &&
          !(error instanceof TransientInfrastructureError) &&
          !(error instanceof ApplicationError) &&
          !this.isTransient(error);
        if (!permanent) throw error;
        result = { transactionId: claimedId!, status: await this.markFailed(claimedId!) };
      }
      if (!result) break;
      results.push(result);
    }
    return results;
  }

  private markFailed(transactionId: string): Promise<string> {
    return this.uow.run(async (scope) => {
      const tx = await scope.transactions.findById(transactionId);
      if (!tx || tx.status !== WagerTransactionStatus.PendingReference) return tx?.status ?? 'UNKNOWN';
      tx.fail(FailureCode.ProcessingFailed, this.clock.now());
      await scope.transactions.save(tx);
      return tx.status;
    });
  }
}
