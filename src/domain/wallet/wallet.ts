import { CurrencyMismatchError, InvalidEntityError, NegativeBalanceError } from '../shared/domain-error';
import { Money } from '../shared/money';
import { LedgerDirection } from './ledger-direction';
import { WalletLedgerEntry } from './wallet-ledger-entry';

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

/** Identificadores do movimento: gerados fora do domínio (IdGenerator) para manter o domínio determinístico. */
export interface MovementProps {
  money: Money;
  transactionId: string;
  ledgerEntryId: string;
  at: Date;
}

export interface OpenWalletProps {
  id: string;
  playerId: string;
  initialBalance: Money;
  /** Obrigatórios quando initialBalance > 0: a abertura gera a transação OPENING + lançamento. */
  openingTransactionId: string;
  openingLedgerEntryId: string;
  at: Date;
}

export interface OpenedWallet {
  wallet: Wallet;
  /** Lançamento de abertura (wallet_version 1). Ausente quando o saldo inicial é zero. */
  openingEntry: WalletLedgerEntry | undefined;
}

/**
 * Aggregate root. O saldo só muda por debit/credit, e cada um devolve o lançamento
 * correspondente, construído a partir do mesmo before/after que alterou o saldo.
 * Assim saldo e ledger não têm como divergir no código; o banco verifica de novo no commit.
 */
export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  /**
   * version nasce em 1 já com o saldo inicial. Se esse saldo for positivo, o lançamento
   * de abertura também carrega wallet_version 1 (0 → saldo inicial), porque o saldo
   * observável da versão 1 é o saldo inicial.
   */
  static open(props: OpenWalletProps): OpenedWallet {
    if (props.initialBalance.isNegative()) throw new InvalidEntityError('initial balance cannot be negative');
    const wallet = new Wallet(
      props.id,
      props.playerId,
      props.initialBalance.currency,
      props.initialBalance,
      1,
      props.at,
      props.at,
    );
    const openingEntry = props.initialBalance.isPositive()
      ? WalletLedgerEntry.create({
          id: props.openingLedgerEntryId,
          walletId: props.id,
          transactionId: props.openingTransactionId,
          walletVersion: 1,
          direction: LedgerDirection.Credit,
          money: props.initialBalance,
          balanceBefore: Money.zero(props.initialBalance.currency),
          balanceAfter: props.initialBalance,
          createdAt: props.at,
        })
      : undefined;
    return { wallet, openingEntry };
  }

  /** Reconstrução a partir da persistência: não revalida transições. */
  static rehydrate(state: WalletState): Wallet {
    return new Wallet(
      state.id,
      state.playerId,
      state.currency,
      state.balance,
      state.version,
      state.createdAt,
      state.updatedAt,
    );
  }

  get balance(): Money {
    return this._balance;
  }

  get version(): number {
    return this._version;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  canDebit(money: Money): boolean {
    this.assertSameCurrency(money);
    return !this._balance.isLessThan(money);
  }

  debit(props: MovementProps): WalletLedgerEntry {
    return this.apply(LedgerDirection.Debit, props);
  }

  credit(props: MovementProps): WalletLedgerEntry {
    return this.apply(LedgerDirection.Credit, props);
  }

  move(direction: LedgerDirection, props: MovementProps): WalletLedgerEntry {
    return this.apply(direction, props);
  }

  private apply(direction: LedgerDirection, { money, transactionId, ledgerEntryId, at }: MovementProps): WalletLedgerEntry {
    this.assertSameCurrency(money);
    if (!money.isPositive()) throw new InvalidEntityError('movement amount must be positive');

    const before = this._balance;
    const after = direction === LedgerDirection.Debit ? before.subtract(money) : before.add(money);
    if (after.isNegative()) {
      // Guarda de invariante. O caminho de negócio é a política rejeitar antes (INSUFFICIENT_FUNDS).
      throw new NegativeBalanceError(`wallet ${this.id}: ${before} - ${money} would be negative`);
    }

    const entry = WalletLedgerEntry.create({
      id: ledgerEntryId,
      walletId: this.id,
      transactionId,
      walletVersion: this._version + 1,
      direction,
      money,
      balanceBefore: before,
      balanceAfter: after,
      createdAt: at,
    });

    // Só altera o estado depois que o lançamento foi validado: falha não deixa a wallet pela metade.
    this._balance = after;
    this._version += 1; // incrementa SOMENTE quando o saldo muda
    this._updatedAt = at;
    return entry;
  }

  private assertSameCurrency(money: Money): void {
    if (money.currency !== this.currency) {
      throw new CurrencyMismatchError(`wallet ${this.id} is ${this.currency}, got ${money.currency}`);
    }
  }
}
