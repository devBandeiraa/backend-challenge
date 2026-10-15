import { LedgerDirection } from '../wallet/ledger-direction';
import type { Wallet } from '../wallet/wallet';
import { FailureCode } from './failure-code';
import type { WagerTransaction } from './wager-transaction';
import { WagerTransactionKind, WagerTransactionStatus } from './wager-transaction-kind';

export type SettlementDecision =
  | { readonly type: 'apply'; readonly direction: LedgerDirection; readonly referenceTransactionId: string | undefined }
  | { readonly type: 'no-effect'; readonly referenceTransactionId: string | undefined }
  | { readonly type: 'reject'; readonly code: FailureCode; readonly referenceTransactionId: string | undefined }
  | { readonly type: 'await-reference' };

export interface SettlementInput {
  transaction: WagerTransaction;
  /** Já travada (FOR NO KEY UPDATE): o saldo lido aqui não muda até o commit. */
  wallet: Wallet;
  /** Resolvida por (providerId, referenceExternalTransactionId); undefined se ainda não chegou. */
  reference: WagerTransaction | undefined;
  /** Existe reversão PROCESSED para a referência (qualquer kind, decisão B). */
  referenceAlreadyReversed: boolean;
}

/** Kinds que cada operação pode referenciar. WIN/LOSS referenciam opcionalmente a BET da rodada. */
const ALLOWED_REFERENCE_KINDS: Partial<Record<WagerTransactionKind, readonly WagerTransactionKind[]>> = {
  [WagerTransactionKind.Refund]: [WagerTransactionKind.Bet],
  [WagerTransactionKind.Rollback]: [WagerTransactionKind.Bet, WagerTransactionKind.Win, WagerTransactionKind.Refund],
  [WagerTransactionKind.Win]: [WagerTransactionKind.Bet],
  [WagerTransactionKind.Loss]: [WagerTransactionKind.Bet],
};

/**
 * Regras de negócio de BET/WIN/LOSS/REFUND/ROLLBACK como função pura: decide, não executa.
 * A ordem das verificações é deliberada: primeiro o que invalida a operação para sempre
 * (player/moeda/contexto/kind), depois o que depende do estado da referência, e por último o saldo.
 */
export class SettlementPolicy {
  decide({ transaction: tx, wallet, reference, referenceAlreadyReversed }: SettlementInput): SettlementDecision {
    if (wallet.playerId !== tx.playerId) return reject(FailureCode.PlayerMismatch);
    if (tx.money.currency !== wallet.currency) return reject(FailureCode.CurrencyMismatch);

    let referenceTransactionId: string | undefined;
    if (tx.referenceExternalTransactionId !== undefined) {
      if (!reference) return { type: 'await-reference' };

      // Sem id da referência aqui: ela pode ser de outra wallet (FK composta impediria gravar).
      if (!sharesContext(tx, reference)) return reject(FailureCode.ReferenceMismatch);
      referenceTransactionId = reference.id;

      if (!ALLOWED_REFERENCE_KINDS[tx.kind]?.includes(reference.kind)) {
        return reject(FailureCode.InvalidReferenceKind, referenceTransactionId);
      }
      if (reference.status === WagerTransactionStatus.Pending || reference.status === WagerTransactionStatus.PendingReference) {
        // Referência ainda não resolvida (ex.: ROLLBACK de um REFUND pendente): espera junto.
        return { type: 'await-reference' };
      }
      if (reference.status !== WagerTransactionStatus.Processed) {
        return reject(FailureCode.ReferenceNotProcessed, referenceTransactionId);
      }
      if (tx.isReversal()) {
        if (!tx.money.equals(reference.money)) return reject(FailureCode.AmountMismatch, referenceTransactionId);
        if (referenceAlreadyReversed) return reject(FailureCode.ReferenceAlreadyReversed, referenceTransactionId);
      }
    }

    if (!tx.affectsBalance()) return { type: 'no-effect', referenceTransactionId };

    const direction = tx.ledgerDirectionFor(reference);
    if (direction === LedgerDirection.Debit && !wallet.canDebit(tx.money)) {
      // Códigos distintos: aposta sem saldo é rotina; reversão que estouraria o saldo é anomalia operacional.
      return reject(tx.isReversal() ? FailureCode.ReversalWouldOverdraw : FailureCode.InsufficientFunds, referenceTransactionId);
    }
    return { type: 'apply', direction, referenceTransactionId };
  }
}

function reject(code: FailureCode, referenceTransactionId?: string): SettlementDecision {
  return { type: 'reject', code, referenceTransactionId };
}

/** Mesmo provider, player, wallet, moeda e rodada. */
function sharesContext(tx: WagerTransaction, reference: WagerTransaction): boolean {
  return (
    reference.providerId === tx.providerId &&
    reference.playerId === tx.playerId &&
    reference.walletId === tx.walletId &&
    reference.money.currency === tx.money.currency &&
    reference.roundId === tx.roundId
  );
}
