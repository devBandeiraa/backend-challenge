import { hashWagerPayload } from '../../src/application/idempotency/payload-hash';
import { Money } from '../../src/domain/shared/money';
import { type CreateWagerTransactionProps, WagerTransaction } from '../../src/domain/wagering/wager-transaction';
import { WagerTransactionKind } from '../../src/domain/wagering/wager-transaction-kind';
import { Wallet } from '../../src/domain/wallet/wallet';

export const T0 = new Date('2026-10-15T12:00:00.000Z');
export const id = () => crypto.randomUUID();
export const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });
export const usd = (amount: string) => Money.from({ amount, currency: 'USD' });

export function openWallet(balance = '100.00', overrides: { playerId?: string; currency?: string } = {}) {
  const money = Money.from({ amount: balance, currency: overrides.currency ?? 'BRL' });
  return Wallet.open({
    id: id(),
    playerId: overrides.playerId ?? id(),
    initialBalance: money,
    openingTransactionId: id(),
    openingLedgerEntryId: id(),
    at: T0,
  }).wallet;
}

export function newTx(
  wallet: Wallet,
  kind: WagerTransactionKind,
  overrides: Partial<CreateWagerTransactionProps> & { amount?: string } = {},
): WagerTransaction {
  const externalTransactionId = overrides.externalTransactionId ?? `ext-${id()}`;
  const money = overrides.money ?? Money.from({ amount: overrides.amount ?? '10.00', currency: wallet.currency });
  const base = {
    providerId: overrides.providerId ?? 'provider-a',
    externalTransactionId,
    playerId: overrides.playerId ?? wallet.playerId,
    walletId: overrides.walletId ?? wallet.id,
    roundId: overrides.roundId ?? 'round-1',
    gameId: overrides.gameId ?? 'fortune-chimp',
    kind,
    money,
    referenceExternalTransactionId: overrides.referenceExternalTransactionId,
  };
  return WagerTransaction.create({
    ...base,
    id: overrides.id ?? id(),
    idempotencyKey: overrides.idempotencyKey ?? `${base.providerId}:${externalTransactionId}`,
    payloadHash: overrides.payloadHash ?? hashWagerPayload({ ...base, money: money.toJSON() }),
    correlationId: overrides.correlationId ?? 'corr-1',
    createdAt: overrides.createdAt ?? T0,
  });
}

/** Transação já PROCESSED, como se viesse do banco (para servir de referência). */
export function processed(tx: WagerTransaction, walletVersion = 2): WagerTransaction {
  tx.markProcessed(tx.requiresReference() ? id() : undefined, T0, { balance: brl('0.00'), walletVersion });
  return tx;
}
