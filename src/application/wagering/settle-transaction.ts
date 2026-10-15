import type { IntegrationEvent } from '../../domain/events/integration-event';
import {
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
} from '../../domain/events/wagering-events';
import { PENDING_REFERENCE_BACKOFF } from '../../domain/messaging/backoff-policy';
import { OutboxMessage } from '../../domain/messaging/outbox-message';
import { SettlementPolicy } from '../../domain/wagering/settlement-policy';
import type { WagerTransaction } from '../../domain/wagering/wager-transaction';
import type { Wallet } from '../../domain/wallet/wallet';
import type { Clock, IdGenerator, TransactionScope } from '../ports';

export interface SettleContext {
  correlationId: string;
  causationId?: string | undefined;
}

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

    switch (decision.type) {
      case 'apply': {
        const entry = wallet.move(decision.direction, {
          money: tx.money,
          transactionId: tx.id,
          ledgerEntryId: this.ids.next(),
          at: now,
        });
        tx.markProcessed(decision.referenceTransactionId, now, { balance: wallet.balance, walletVersion: wallet.version });
        // Ordem respeita as FKs/triggers: wallet e transação antes do lançamento que as referencia.
        await scope.wallets.saveBalance(wallet);
        await scope.transactions.save(tx);
        await scope.ledger.append(entry);
        events.push(WagerTransactionProcessed.from(tx, eventCtx()), WalletBalanceChanged.from(wallet, entry, eventCtx()));
        break;
      }
      case 'no-effect':
        // LOSS: PROCESSED, sem ledger e sem WalletBalanceChanged; version não muda.
        tx.markProcessed(decision.referenceTransactionId, now, { balance: wallet.balance, walletVersion: wallet.version });
        await scope.transactions.save(tx);
        events.push(WagerTransactionProcessed.from(tx, eventCtx()));
        break;
      case 'reject':
        tx.reject(decision.code, now, { balance: wallet.balance, walletVersion: wallet.version }, decision.referenceTransactionId);
        await scope.transactions.save(tx);
        events.push(WagerTransactionRejected.from(tx, eventCtx()));
        break;
      case 'await-reference':
        tx.markPendingReference(PENDING_REFERENCE_BACKOFF.nextAttemptAt(now, tx.attempts + 1), now);
        await scope.transactions.save(tx);
        events.push(WagerTransactionPendingReference.from(tx, eventCtx()));
        break;
    }

    await scope.outbox.enqueue(events.map((e) => OutboxMessage.enqueue(e)));
  }
}
