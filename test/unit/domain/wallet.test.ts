import { describe, expect, test } from 'bun:test';
import {
  CurrencyMismatchError,
  InvalidEntityError,
  NegativeBalanceError,
} from '../../../src/domain/shared/domain-error';
import { Money } from '../../../src/domain/shared/money';
import { LedgerDirection } from '../../../src/domain/wallet/ledger-direction';
import { Wallet } from '../../../src/domain/wallet/wallet';
import { WalletLedgerEntry } from '../../../src/domain/wallet/wallet-ledger-entry';
import { brl, id, openWallet, T0, usd } from '../../support/builders';

const T1 = new Date(T0.getTime() + 1000);
const move = (money: Money) => ({ money, transactionId: id(), ledgerEntryId: id(), at: T1 });

/** Invariante dos testes: saldo == soma dos lançamentos. */
function rebuild(currency: string, entries: WalletLedgerEntry[]): Money {
  return entries.reduce((acc, e) => acc.add(e.signedAmount()), Money.zero(currency));
}

describe('Wallet.open', () => {
  test('saldo inicial positivo: version 1 e lançamento de abertura CREDIT 0 → saldo (wallet_version 1)', () => {
    const { wallet, openingEntry } = Wallet.open({
      id: id(),
      playerId: id(),
      initialBalance: brl('1000.00'),
      openingTransactionId: id(),
      openingLedgerEntryId: id(),
      at: T0,
    });
    expect(wallet.version).toBe(1);
    expect(wallet.balance.amount).toBe('1000.00');
    expect(openingEntry?.direction).toBe(LedgerDirection.Credit);
    expect(openingEntry?.balanceBefore.amount).toBe('0.00');
    expect(openingEntry?.balanceAfter.amount).toBe('1000.00');
    expect(openingEntry?.walletVersion).toBe(1);
  });

  test('saldo inicial zero: version 1 e nenhum lançamento', () => {
    const { wallet, openingEntry } = Wallet.open({
      id: id(),
      playerId: id(),
      initialBalance: brl('0.00'),
      openingTransactionId: id(),
      openingLedgerEntryId: id(),
      at: T0,
    });
    expect(wallet.version).toBe(1);
    expect(openingEntry).toBeUndefined();
  });

  test('moeda da wallet vem do saldo inicial', () => {
    expect(openWallet('5.00', { currency: 'USD' }).currency).toBe('USD');
  });
});

describe('Wallet: débito e crédito', () => {
  test('debit reduz saldo, incrementa version e devolve lançamento consistente', () => {
    const wallet = openWallet('100.00');
    const entry = wallet.debit(move(brl('30.00')));
    expect(wallet.balance.amount).toBe('70.00');
    expect(wallet.version).toBe(2);
    expect(wallet.updatedAt).toEqual(T1);
    expect(entry.direction).toBe(LedgerDirection.Debit);
    expect(entry.balanceBefore.amount).toBe('100.00');
    expect(entry.balanceAfter.amount).toBe('70.00');
    expect(entry.walletVersion).toBe(2);
    expect(entry.isBalanced()).toBe(true);
  });

  test('credit aumenta saldo e incrementa version', () => {
    const wallet = openWallet('0.00');
    const entry = wallet.credit(move(brl('12.34')));
    expect(wallet.balance.amount).toBe('12.34');
    expect(wallet.version).toBe(2);
    expect(entry.balanceAfter.equals(wallet.balance)).toBe(true);
  });

  test('debitar exatamente o saldo zera a wallet (não é negativo)', () => {
    const wallet = openWallet('80.00');
    wallet.debit(move(brl('80.00')));
    expect(wallet.balance.amount).toBe('0.00');
  });

  test('saldo nunca negativo: debit maior que o saldo lança e NÃO altera estado', () => {
    const wallet = openWallet('100.00');
    expect(() => wallet.debit(move(brl('100.01')))).toThrow(NegativeBalanceError);
    expect(wallet.balance.amount).toBe('100.00');
    expect(wallet.version).toBe(1);
  });

  test('canDebit reflete o saldo', () => {
    const wallet = openWallet('100.00');
    expect(wallet.canDebit(brl('100.00'))).toBe(true);
    expect(wallet.canDebit(brl('100.01'))).toBe(false);
  });

  test('conflito de moeda: operação USD numa wallet BRL lança e não altera estado', () => {
    const wallet = openWallet('100.00');
    expect(() => wallet.credit(move(usd('1.00')))).toThrow(CurrencyMismatchError);
    expect(() => wallet.canDebit(usd('1.00'))).toThrow(CurrencyMismatchError);
    expect(wallet.version).toBe(1);
  });

  test('movimento zero é rejeitado (não gera lançamento nem muda version)', () => {
    const wallet = openWallet('100.00');
    expect(() => wallet.credit(move(brl('0.00')))).toThrow(InvalidEntityError);
    expect(wallet.version).toBe(1);
  });

  test('sequência de movimentos: version conta mudanças e saldo == ledger reconstruído', () => {
    const { wallet, openingEntry } = Wallet.open({
      id: id(),
      playerId: id(),
      initialBalance: brl('100.00'),
      openingTransactionId: id(),
      openingLedgerEntryId: id(),
      at: T0,
    });
    const entries = [openingEntry!];
    entries.push(wallet.debit(move(brl('25.00'))));
    entries.push(wallet.credit(move(brl('50.00'))));
    entries.push(wallet.debit(move(brl('125.00'))));
    expect(wallet.balance.amount).toBe('0.00');
    expect(wallet.version).toBe(4);
    expect(entries.map((e) => e.walletVersion)).toEqual([1, 2, 3, 4]);
    // encadeamento: before de cada lançamento == after do anterior
    for (let i = 1; i < entries.length; i++) {
      expect(entries[i]!.balanceBefore.equals(entries[i - 1]!.balanceAfter)).toBe(true);
    }
    expect(rebuild('BRL', entries).equals(wallet.balance)).toBe(true);
  });

  test('rehydrate reconstrói estado sem revalidar', () => {
    const wallet = Wallet.rehydrate({
      id: id(),
      playerId: id(),
      currency: 'BRL',
      balance: brl('42.00'),
      version: 7,
      createdAt: T0,
      updatedAt: T1,
    });
    expect(wallet.balance.amount).toBe('42.00');
    expect(wallet.version).toBe(7);
  });
});

describe('WalletLedgerEntry', () => {
  const base = {
    id: id(),
    walletId: id(),
    transactionId: id(),
    walletVersion: 2,
    direction: LedgerDirection.Debit,
    money: brl('10.00'),
    balanceBefore: brl('50.00'),
    balanceAfter: brl('40.00'),
    createdAt: T0,
  };

  test('create valida a aritmética', () => {
    expect(WalletLedgerEntry.create(base).isBalanced()).toBe(true);
    expect(() => WalletLedgerEntry.create({ ...base, balanceAfter: brl('41.00') })).toThrow(InvalidEntityError);
    expect(() => WalletLedgerEntry.create({ ...base, direction: LedgerDirection.Credit })).toThrow(InvalidEntityError);
  });

  test('rejeita valor não positivo, versão inválida e moedas misturadas', () => {
    expect(() => WalletLedgerEntry.create({ ...base, money: brl('0.00'), balanceAfter: brl('50.00') })).toThrow(
      InvalidEntityError,
    );
    expect(() => WalletLedgerEntry.create({ ...base, walletVersion: 0 })).toThrow(InvalidEntityError);
    expect(() => WalletLedgerEntry.create({ ...base, money: usd('10.00') })).toThrow(CurrencyMismatchError);
  });

  test('imutabilidade estrutural: objeto congelado', () => {
    const entry = WalletLedgerEntry.create(base);
    expect(Object.isFrozen(entry)).toBe(true);
    expect(() => {
      (entry as unknown as { walletVersion: number }).walletVersion = 99;
    }).toThrow();
  });
});
