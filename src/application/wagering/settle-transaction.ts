import type { IntegrationEvent } from '../../domain/events/integration-event';
import {
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
} from '../../domain/events/wagering-events';
import {
  type ExponentialBackoff,
  PENDING_REFERENCE_BACKOFF,
  PENDING_REFERENCE_MAX_ATTEMPTS,
} from '../../domain/messaging/backoff-policy';
import { OutboxMessage } from '../../domain/messaging/outbox-message';
import { FailureCode } from '../../domain/wagering/failure-code';
import { SettlementPolicy } from '../../domain/wagering/settlement-policy';
import type { WagerTransaction } from '../../domain/wagering/wager-transaction';
import { WagerTransactionStatus } from '../../domain/wagering/wager-transaction-kind';
import type { Wallet } from '../../domain/wallet/wallet';
import type { Clock, IdGenerator, TransactionScope } from '../ports';

export interface SettleContext {
  correlationId: string;
  causationId?: string | undefined;
}

/** Janela de espera por referência fora de ordem (configurável para testes). */
export interface PendingReferencePolicy {
  backoff: ExponentialBackoff;
  /** Total de tentativas de resolução, contando o processamento inicial. */
  maxAttempts: number;
}

export const DEFAULT_PENDING_REFERENCE_POLICY: PendingReferencePolicy = {
  backoff: PENDING_REFERENCE_BACKOFF,
  maxAttempts: PENDING_REFERENCE_MAX_ATTEMPTS,
};

/**
 * Aplica a decisão da política a uma transação PENDING/PENDING_REFERENCE com a wallet JÁ
 * travada, e grava tudo no mesmo scope: wallet, ledger, transação e outbox.
 * Compartilhado entre a submissão (HTTP/SQS) e o worker de PENDING_REFERENCE.
 */
export class TransactionSettler {
  private readonly policy = new SettlementPolicy();

  constructor(
    private readonly clock: Clock,
    private readonly ids: IdGenerator,
    private readonly pending: PendingReferencePolicy = DEFAULT_PENDING_REFERENCE_POLICY,
  ) {}

  async settle(scope: TransactionScope, tx: WagerTransaction, wallet: Wallet, ctx: SettleContext): Promise<void> {
    const now = this.clock.now();
    const reference = tx.referenceExternalTransactionId
      ? await scope.transactions.findByExternalId(tx.providerId, tx.referenceExternalTransactionId)
      : undefined;
    const referenceAlreadyReversed =
      reference !== undefined && tx.isReversal() ? await scope.transactions.hasProcessedReversal(reference.id) : false;

    const decision = this.policy.decide({ transaction: tx, wallet, reference, referenceAlreadyReversed });
    const events: IntegrationEvent<unknown>[] = [];
    const eventCtx = () => ({ eventId: this.ids.next(), ...ctx, occurredAt: now });
    const snapshot = () => ({ balance: wallet.balance, walletVersion: wallet.version });

    switch (decision.type) {
      case 'apply': {
        const entry = wallet.move(decision.direction, {
          money: tx.money,
          transactionId: tx.id,
          ledgerEntryId: this.ids.next(),
          at: now,
        });
        tx.markProcessed(decision.referenceTransactionId, now, snapshot());
        // Ordem respeita as FKs/triggers: wallet e transação antes do lançamento que as referencia.
        await scope.wallets.saveBalance(wallet);
        await scope.transactions.save(tx);
        await scope.ledger.append(entry);
        events.push(WagerTransactionProcessed.from(tx, eventCtx()), WalletBalanceChanged.from(wallet, entry, eventCtx()));
        break;
      }
      case 'no-effect':
        // LOSS: PROCESSED, sem ledger e sem WalletBalanceChanged; version não muda.
        tx.markProcessed(decision.referenceTransactionId, now, snapshot());
        await scope.transactions.save(tx);
        events.push(WagerTransactionProcessed.from(tx, eventCtx()));
        break;
      case 'reject':
        tx.reject(decision.code, now, snapshot(), decision.referenceTransactionId);
        await scope.transactions.save(tx);
        events.push(WagerTransactionRejected.from(tx, eventCtx()));
        break;
      case 'await-reference': {
        const attempt = tx.attempts + 1; // esta tentativa
        if (attempt >= this.pending.maxAttempts) {
          // Janela esgotada: rejeição terminal e auditável, com evento (README 7.1).
          tx.reject(FailureCode.ReferenceNotFound, now, snapshot());
          await scope.transactions.save(tx);
          events.push(WagerTransactionRejected.from(tx, eventCtx()));
        } else {
          // Evento só na ENTRADA em PENDING_REFERENCE; reagendamentos do worker não repetem o evento.
          const entering = tx.status === WagerTransactionStatus.Pending;
          tx.markPendingReference(this.pending.backoff.nextAttemptAt(now, attempt), now);
          await scope.transactions.save(tx);
          if (entering) events.push(WagerTransactionPendingReference.from(tx, eventCtx()));
        }
        break;
      }
    }

    await scope.outbox.enqueue(events.map((e) => OutboxMessage.enqueue(e)));
  }
}
