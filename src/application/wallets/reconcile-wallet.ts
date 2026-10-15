import type { MoneyProps } from '../../domain/shared/money';
import { NotFoundError } from '../errors';
import type { UnitOfWork } from '../ports';

export interface ReconciliationReport {
  walletId: string;
  storedBalance: MoneyProps;
  calculatedBalance: MoneyProps;
  /** stored − calculated (pode ser negativo). */
  difference: MoneyProps;
  /** Saldo E versão batem com o ledger. */
  consistent: boolean;
  checkedEntries: number;
  walletVersion: number;
  /** wallet_version do último lançamento (1 quando não há lançamentos). */
  ledgerVersion: number;
}

/** Porta de observação: log/métrica são infraestrutura; a aplicação só sinaliza. */
export interface ReconciliationObserver {
  onMismatch(report: ReconciliationReport): void;
}

/**
 * Compara o saldo materializado com o saldo reconstruído pelo ledger. NÃO corrige: divergência
 * é sintoma de bug ou de escrita fora da aplicação, e "consertar" silenciosamente apagaria a
 * evidência. Loga, conta em métrica e devolve `consistent: false` para alguém investigar.
 * O snapshot vem de UMA instrução SQL (wallet + agregados do ledger): consistente mesmo em
 * READ COMMITTED e sem travar a wallet (não bloqueia o fluxo financeiro).
 */
export class ReconcileWalletUseCase {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly observer: ReconciliationObserver,
  ) {}

  async execute(walletId: string): Promise<ReconciliationReport> {
    const snapshot = await this.uow.run((scope) => scope.wallets.reconciliationSnapshot(walletId));
    if (!snapshot) throw new NotFoundError(`wallet ${walletId} not found`);

    const { wallet, credits, debits, entries, lastLedgerVersion } = snapshot;
    const calculated = credits.subtract(debits);
    const difference = wallet.balance.subtract(calculated);
    const ledgerVersion = lastLedgerVersion ?? 1;
    const report: ReconciliationReport = {
      walletId,
      storedBalance: wallet.balance.toJSON(),
      calculatedBalance: calculated.toJSON(),
      difference: difference.toJSON(),
      consistent: difference.isZero() && wallet.version === ledgerVersion,
      checkedEntries: entries,
      walletVersion: wallet.version,
      ledgerVersion,
    };
    if (!report.consistent) this.observer.onMismatch(report);
    return report;
  }
}
