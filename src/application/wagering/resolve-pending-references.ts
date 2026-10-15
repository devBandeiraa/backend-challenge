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
 */
export class ResolvePendingReferencesUseCase {
  private readonly settler: TransactionSettler;

  constructor(
    private readonly uow: UnitOfWork,
    private readonly clock: Clock,
    ids: IdGenerator,
    policy: PendingReferencePolicy = DEFAULT_PENDING_REFERENCE_POLICY,
  ) {
    this.settler = new TransactionSettler(clock, ids, policy);
  }

  /** Processa até `limit` transações vencidas. */
  async runOnce(limit: number): Promise<ResolveResult[]> {
    const results: ResolveResult[] = [];
    for (let i = 0; i < limit; i++) {
      const result = await this.uow.run(async (scope) => {
        const tx = await scope.transactions.claimDuePendingReference(this.clock.now());
        if (!tx) return undefined;
        const wallet = await scope.wallets.lockById(tx.walletId);
        if (!wallet) throw new Error(`wallet ${tx.walletId} vanished`); // impossível: sem DELETE
        await this.settler.settle(scope, tx, wallet, { correlationId: tx.correlationId, causationId: tx.id });
        return { transactionId: tx.id, status: tx.status };
      });
      if (!result) break;
      results.push(result);
    }
    return results;
  }
}
