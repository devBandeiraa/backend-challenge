import { Migration } from '@mikro-orm/migrations';

/** Papel de privilégios da aplicação (NOLOGIN). O usuário de login herda dele (ver migrate.ts). */
export const APP_PRIVILEGES_ROLE = 'wagering_app_rw';

/**
 * Menor privilégio: a aplicação não é superuser nem dona das tabelas. Assim a imutabilidade do
 * ledger não depende só dos triggers: com as credenciais da app, UPDATE/DELETE/TRUNCATE no ledger,
 * ALTER TABLE ... DISABLE TRIGGER e SET session_replication_role são recusados pelo próprio Postgres.
 *
 *  - ledger:     SELECT, INSERT (append-only também por privilégio);
 *  - demais:     SELECT, INSERT e UPDATE SÓ das colunas mutáveis (identidade e campos de negócio
 *                ficam fora do alcance da app, além dos triggers);
 *  - nenhum DELETE, TRUNCATE, REFERENCES ou TRIGGER; nenhum CREATE no schema.
 *
 * O papel é do cluster (não do database). Ele só é criado se faltar, e o down só o remove quando
 * nenhum outro database ainda concede privilégios a ele. A senha de login não fica aqui: é
 * configuração operacional, provisionada pelo migrate.ts a partir de DATABASE_URL.
 * Tabela nova em migration futura precisa de GRANT explícito para este papel.
 */
export class Migration008AppRoleLeastPrivilege extends Migration {
  override name = '008-app-role-least-privilege';

  override up(): void {
    this.addSql(`
      DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${APP_PRIVILEGES_ROLE}') THEN
          CREATE ROLE ${APP_PRIVILEGES_ROLE} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
        END IF;
      END $$`);
    this.addSql(`GRANT USAGE ON SCHEMA public TO ${APP_PRIVILEGES_ROLE}`);

    this.addSql(`GRANT SELECT, INSERT ON wallet_ledger_entries TO ${APP_PRIVILEGES_ROLE}`);

    this.addSql(`GRANT SELECT, INSERT ON wallets TO ${APP_PRIVILEGES_ROLE}`);
    this.addSql(`GRANT UPDATE (balance, version, updated_at) ON wallets TO ${APP_PRIVILEGES_ROLE}`);

    this.addSql(`GRANT SELECT, INSERT ON wager_transactions TO ${APP_PRIVILEGES_ROLE}`);
    this.addSql(`GRANT UPDATE (status, reference_transaction_id, failure_code, result_balance, result_balance_currency,
                               result_wallet_version, attempts, next_attempt_at, processed_at, updated_at)
                 ON wager_transactions TO ${APP_PRIVILEGES_ROLE}`);

    this.addSql(`GRANT SELECT, INSERT ON inbox_messages TO ${APP_PRIVILEGES_ROLE}`);
    this.addSql(`GRANT UPDATE (transaction_id, processed_at) ON inbox_messages TO ${APP_PRIVILEGES_ROLE}`);

    this.addSql(`GRANT SELECT, INSERT ON outbox_messages TO ${APP_PRIVILEGES_ROLE}`);
    this.addSql(`GRANT UPDATE (attempts, next_attempt_at, published_at, last_error) ON outbox_messages TO ${APP_PRIVILEGES_ROLE}`);
  }

  override down(): void {
    this.addSql(`REVOKE ALL ON ALL TABLES IN SCHEMA public FROM ${APP_PRIVILEGES_ROLE}`);
    this.addSql(`REVOKE USAGE ON SCHEMA public FROM ${APP_PRIVILEGES_ROLE}`);
    // Outro database do mesmo cluster ainda usando o papel: mantém o papel, revoga só aqui.
    this.addSql(`
      DO $$ BEGIN
        DROP ROLE IF EXISTS ${APP_PRIVILEGES_ROLE};
      EXCEPTION WHEN dependent_objects_still_exist THEN
        RAISE NOTICE 'role ${APP_PRIVILEGES_ROLE} still used by another database; kept';
      END $$`);
  }
}
