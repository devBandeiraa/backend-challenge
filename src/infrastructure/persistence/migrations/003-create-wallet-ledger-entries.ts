import { Migration } from '@mikro-orm/migrations';

export class Migration003CreateWalletLedgerEntries extends Migration {
  override name = '003-create-wallet-ledger-entries';

  override up(): void {
    this.addSql(`
      CREATE TABLE wallet_ledger_entries (
        id              uuid          PRIMARY KEY,
        wallet_id       uuid          NOT NULL,
        transaction_id  uuid          NOT NULL,
        -- Versão da wallet APÓS o lançamento: encadeia o ledger e serve de cursor estável.
        wallet_version  integer       NOT NULL,
        direction       varchar(6)    NOT NULL,
        amount          numeric(19,2) NOT NULL,
        currency        char(3)       NOT NULL,
        balance_before  numeric(19,2) NOT NULL,
        balance_after   numeric(19,2) NOT NULL,
        created_at      timestamptz   NOT NULL DEFAULT now(),
        CONSTRAINT le_wallet_currency_fk    FOREIGN KEY (wallet_id, currency)       REFERENCES wallets(id, currency),
        CONSTRAINT le_transaction_fk        FOREIGN KEY (transaction_id, wallet_id) REFERENCES wager_transactions(id, wallet_id),
        CONSTRAINT le_transaction_wallet_uq UNIQUE (transaction_id, wallet_id),
        CONSTRAINT le_wallet_version_uq     UNIQUE (wallet_id, wallet_version),
        CONSTRAINT le_direction_chk         CHECK (direction IN ('DEBIT','CREDIT')),
        CONSTRAINT le_amount_positive_chk   CHECK (amount > 0),
        CONSTRAINT le_balances_non_neg_chk  CHECK (balance_before >= 0 AND balance_after >= 0),
        CONSTRAINT le_version_chk           CHECK (wallet_version >= 1),
        CONSTRAINT le_arithmetic_chk        CHECK (
             (direction = 'CREDIT' AND balance_after = balance_before + amount)
          OR (direction = 'DEBIT'  AND balance_after = balance_before - amount))
      )`);
  }

  override down(): void {
    this.addSql('DROP TABLE wallet_ledger_entries');
  }
}
