import { Migration } from '@mikro-orm/migrations';

/**
 * Coerência lançamento ↔ transação no schema (antes só no código). Todo lançamento precisa
 * apontar para uma transação que de fato move saldo, com o mesmo valor e moeda e na direção do kind:
 *   - status PROCESSED (REJECTED/FAILED/pendentes não geram lançamento);
 *   - kind ≠ LOSS (LOSS não move saldo);
 *   - amount e currency iguais aos da transação;
 *   - direção: BET → DEBIT; OPENING/WIN/REFUND → CREDIT; ROLLBACK → inverso da referência
 *     (ROLLBACK de BET credita; de WIN/REFUND debita).
 * BEFORE INSERT: roda na mesma transação que marcou a transação como PROCESSED (o caso de uso grava a
 * transação antes do lançamento). SQLSTATE próprio WL005.
 */
export class Migration009LedgerMatchesTransaction extends Migration {
  override name = '009-ledger-matches-transaction';

  override up(): void {
    this.addSql(`
      CREATE FUNCTION ledger_assert_matches_transaction() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE t record; expected text; reference_kind text;
      BEGIN
        SELECT status, kind, amount, currency, reference_transaction_id INTO t
          FROM wager_transactions WHERE id = NEW.transaction_id AND wallet_id = NEW.wallet_id;
        IF NOT FOUND THEN
          RAISE EXCEPTION 'ledger entry references unknown transaction % of wallet %', NEW.transaction_id, NEW.wallet_id
            USING ERRCODE = 'WL005';
        END IF;
        IF t.status <> 'PROCESSED' THEN
          RAISE EXCEPTION 'ledger entry requires a PROCESSED transaction (% is %)', NEW.transaction_id, t.status USING ERRCODE = 'WL005';
        END IF;
        IF t.kind = 'LOSS' THEN
          RAISE EXCEPTION 'LOSS % does not move balance', NEW.transaction_id USING ERRCODE = 'WL005';
        END IF;
        IF NEW.amount <> t.amount OR NEW.currency <> t.currency THEN
          RAISE EXCEPTION 'ledger amount % % differs from transaction % %', NEW.amount, NEW.currency, t.amount, t.currency
            USING ERRCODE = 'WL005';
        END IF;
        IF t.kind = 'BET' THEN
          expected := 'DEBIT';
        ELSIF t.kind IN ('OPENING', 'WIN', 'REFUND') THEN
          expected := 'CREDIT';
        ELSE -- ROLLBACK: inverso da referência (resolvida, por wt_reversal_resolved_chk)
          SELECT kind INTO reference_kind FROM wager_transactions WHERE id = t.reference_transaction_id;
          expected := CASE WHEN reference_kind = 'BET' THEN 'CREDIT' ELSE 'DEBIT' END;
        END IF;
        IF NEW.direction <> expected THEN
          RAISE EXCEPTION 'ledger direction % does not match % (expected %)', NEW.direction, t.kind, expected USING ERRCODE = 'WL005';
        END IF;
        RETURN NEW;
      END $$`);
    this.addSql(`CREATE TRIGGER ledger_matches_transaction BEFORE INSERT ON wallet_ledger_entries
                 FOR EACH ROW EXECUTE FUNCTION ledger_assert_matches_transaction()`);
  }

  override down(): void {
    this.addSql('DROP TRIGGER ledger_matches_transaction ON wallet_ledger_entries');
    this.addSql('DROP FUNCTION ledger_assert_matches_transaction()');
  }
}
