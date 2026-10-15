import { Migration } from '@mikro-orm/migrations';

export class Migration001CreateWallets extends Migration {
  override name = '001-create-wallets';

  override up(): void {
    this.addSql(`
      CREATE TABLE wallets (
        id          uuid          PRIMARY KEY,
        player_id   uuid          NOT NULL,
        currency    char(3)       NOT NULL,
        balance     numeric(19,2) NOT NULL,
        version     integer       NOT NULL DEFAULT 1,
        created_at  timestamptz   NOT NULL DEFAULT now(),
        updated_at  timestamptz   NOT NULL DEFAULT now(),
        CONSTRAINT wallets_currency_format_chk  CHECK (currency ~ '^[A-Z]{3}$'),
        CONSTRAINT wallets_balance_non_negative CHECK (balance >= 0),
        CONSTRAINT wallets_version_positive     CHECK (version >= 1),
        CONSTRAINT wallets_player_currency_uq   UNIQUE (player_id, currency),
        -- Redundante com a PK, mas é o alvo da FK composta do ledger:
        -- garante no schema que o lançamento está na moeda da wallet.
        CONSTRAINT wallets_id_currency_uq       UNIQUE (id, currency)
      )`);
  }

  override down(): void {
    this.addSql('DROP TABLE wallets');
  }
}
