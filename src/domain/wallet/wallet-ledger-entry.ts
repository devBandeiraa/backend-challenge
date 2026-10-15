import { InvalidEntityError } from '../shared/domain-error';
import { Money } from '../shared/money';
import { LedgerDirection } from './ledger-direction';

export interface LedgerEntryState {
  id: string;
  walletId: string;
  transactionId: string;
  /** Versão da wallet após o lançamento: encadeia o ledger e ordena o cursor. */
  walletVersion: number;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt: Date;
}

export type CreateLedgerEntryProps = LedgerEntryState;

/**
 * Lançamento imutável: todos os campos são readonly e não há métodos de transição.
 * A aritmética é verificada aqui e, de novo, por CHECK no banco (le_arithmetic_chk).
 */
export class WalletLedgerEntry {
  private constructor(
    public readonly id: string,
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly walletVersion: number,
    public readonly direction: LedgerDirection,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
    public readonly createdAt: Date,
  ) {
    Object.freeze(this);
  }

  static create(props: CreateLedgerEntryProps): WalletLedgerEntry {
    const entry = WalletLedgerEntry.rehydrate(props);
    if (!entry.money.isPositive()) throw new InvalidEntityError('ledger amount must be positive');
    if (entry.balanceBefore.isNegative() || entry.balanceAfter.isNegative()) {
      throw new InvalidEntityError('ledger balances must be non-negative');
    }
    if (!Number.isInteger(entry.walletVersion) || entry.walletVersion < 1) {
      throw new InvalidEntityError('walletVersion must be a positive integer');
    }
    // isBalanced também lança CurrencyMismatchError se as moedas divergirem.
    if (!entry.isBalanced()) {
      throw new InvalidEntityError(
        `unbalanced entry: ${entry.balanceBefore} ${entry.direction} ${entry.money} != ${entry.balanceAfter}`,
      );
    }
    return entry;
  }

  static rehydrate(state: LedgerEntryState): WalletLedgerEntry {
    return new WalletLedgerEntry(
      state.id,
      state.walletId,
      state.transactionId,
      state.walletVersion,
      state.direction,
      state.money,
      state.balanceBefore,
      state.balanceAfter,
      state.createdAt,
    );
  }

  /** balanceBefore ± money === balanceAfter */
  isBalanced(): boolean {
    const expected =
      this.direction === LedgerDirection.Credit
        ? this.balanceBefore.add(this.money)
        : this.balanceBefore.subtract(this.money);
    return expected.equals(this.balanceAfter);
  }

  /** Efeito com sinal no saldo; usado para reconstruir o saldo a partir do ledger. */
  signedAmount(): Money {
    return this.direction === LedgerDirection.Credit ? this.money : this.money.negate();
  }
}
