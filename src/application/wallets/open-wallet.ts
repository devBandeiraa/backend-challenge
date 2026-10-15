import { WagerTransactionProcessed, WalletBalanceChanged } from '../../domain/events/wagering-events';
import { OutboxMessage } from '../../domain/messaging/outbox-message';
import { DomainError } from '../../domain/shared/domain-error';
import { Money, type MoneyProps } from '../../domain/shared/money';
import { WagerTransaction } from '../../domain/wagering/wager-transaction';
import { Wallet } from '../../domain/wallet/wallet';
import { ValidationError } from '../errors';
import { sha256Hex } from '../idempotency/payload-hash';
import { canonicalJson } from '../idempotency/canonical-json';
import type { Clock, IdGenerator, UnitOfWork } from '../ports';

export interface OpenWalletCommand {
  playerId: string;
  initialBalance: MoneyProps;
  correlationId: string;
}

export interface WalletView {
  id: string;
  playerId: string;
  balance: MoneyProps;
  version: number;
}

export class OpenWalletUseCase {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly clock: Clock,
    private readonly ids: IdGenerator,
  ) {}

  /**
   * Wallet + OPENING + lançamento + eventos na MESMA transação SQL.
   * Duplicata (player, moeda) vira WalletAlreadyExistsError via UNIQUE do banco: sem check-then-insert.
   */
  async execute(command: OpenWalletCommand): Promise<WalletView> {
    let initialBalance: Money;
    try {
      initialBalance = Money.from(command.initialBalance);
    } catch (error) {
      if (error instanceof DomainError) throw new ValidationError(error.code, error.message);
      throw error;
    }

    return this.uow.run(async (scope) => {
      const now = this.clock.now();
      const walletId = this.ids.next();
      const openingTransactionId = this.ids.next();
      const { wallet, openingEntry } = Wallet.open({
        id: walletId,
        playerId: command.playerId,
        initialBalance,
        openingTransactionId,
        openingLedgerEntryId: this.ids.next(),
        at: now,
      });

      await scope.wallets.insert(wallet);

      if (openingEntry) {
        const opening = WagerTransaction.createOpening({
          id: openingTransactionId,
          walletId,
          playerId: command.playerId,
          money: initialBalance,
          payloadHash: sha256Hex(canonicalJson({ v: 'opening/v1', walletId, money: initialBalance.toJSON() })),
          correlationId: command.correlationId,
          at: now,
        });
        await scope.transactions.insertIfAbsent(opening);
        await scope.ledger.append(openingEntry);
        const ctx = () => ({ eventId: this.ids.next(), correlationId: command.correlationId, occurredAt: now });
        await scope.outbox.enqueue([
          OutboxMessage.enqueue(WagerTransactionProcessed.from(opening, ctx())),
          OutboxMessage.enqueue(WalletBalanceChanged.from(wallet, openingEntry, ctx())),
        ]);
      }

      return toWalletView(wallet);
    });
  }
}

export function toWalletView(wallet: Wallet): WalletView {
  return { id: wallet.id, playerId: wallet.playerId, balance: wallet.balance.toJSON(), version: wallet.version };
}
