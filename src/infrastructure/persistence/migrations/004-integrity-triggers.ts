import { Migration } from '@mikro-orm/migrations';

/**
 * Invariantes que CHECK/UNIQUE não expressam (comparam OLD/NEW ou cruzam tabelas).
 * SQLSTATEs próprios (WL0xx/WT0xx) permitem ao código distinguir "violação de integridade"
 * de erro transitório sem interpretar mensagem.
 */
export class Migration004IntegrityTriggers extends Migration {
  override name = '004-integrity-triggers';

  override up(): void {
    // (a) Ledger append-only.
    this.addSql(`
      CREATE FUNCTION ledger_forbid_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        RAISE EXCEPTION 'wallet_ledger_entries is append-only (% blocked)', TG_OP USING ERRCODE = 'WL001';
      END $$`);
    this.addSql(`CREATE TRIGGER ledger_no_update_delete BEFORE UPDATE OR DELETE ON wallet_ledger_entries
                 FOR EACH ROW EXECUTE FUNCTION ledger_forbid_mutation()`);
    this.addSql(`CREATE TRIGGER ledger_no_truncate BEFORE TRUNCATE ON wallet_ledger_entries
                 FOR EACH STATEMENT EXECUTE FUNCTION ledger_forbid_mutation()`);

    // (b) Encadeamento: balance_before = balance_after do lançamento anterior (ou 0 no primeiro).
    this.addSql(`
      CREATE FUNCTION ledger_assert_chain() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE prev record;
      BEGIN
        SELECT wallet_version, balance_after INTO prev FROM wallet_ledger_entries
         WHERE wallet_id = NEW.wallet_id ORDER BY wallet_version DESC LIMIT 1;
        IF NOT FOUND THEN
          IF NEW.balance_before <> 0 THEN
            RAISE EXCEPTION 'first ledger entry of wallet % must start at 0', NEW.wallet_id USING ERRCODE = 'WL002';
          END IF;
        ELSIF NEW.wallet_version <= prev.wallet_version OR NEW.balance_before <> prev.balance_after THEN
          RAISE EXCEPTION 'ledger chain broken for wallet %', NEW.wallet_id USING ERRCODE = 'WL002';
        END IF;
        RETURN NEW;
      END $$`);
    this.addSql(`CREATE TRIGGER ledger_chain BEFORE INSERT ON wallet_ledger_entries
                 FOR EACH ROW EXECUTE FUNCTION ledger_assert_chain()`);

    // (c) Wallet: identidade imutável, sem DELETE, version = 1 na criação e +1 sse o saldo muda.
    this.addSql(`
      CREATE FUNCTION wallets_guard() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF TG_OP = 'DELETE' THEN
          RAISE EXCEPTION 'wallets cannot be deleted' USING ERRCODE = 'WL003';
        ELSIF TG_OP = 'INSERT' THEN
          IF NEW.version <> 1 THEN
            RAISE EXCEPTION 'wallet version must start at 1' USING ERRCODE = 'WL003';
          END IF;
          RETURN NEW;
        END IF;
        IF (NEW.id, NEW.player_id, NEW.currency, NEW.created_at) IS DISTINCT FROM
           (OLD.id, OLD.player_id, OLD.currency, OLD.created_at) THEN
          RAISE EXCEPTION 'wallet identity is immutable' USING ERRCODE = 'WL003';
        END IF;
        IF (NEW.balance <> OLD.balance AND NEW.version <> OLD.version + 1)
        OR (NEW.balance =  OLD.balance AND NEW.version <> OLD.version) THEN
          RAISE EXCEPTION 'wallet version must increment iff balance changes' USING ERRCODE = 'WL003';
        END IF;
        RETURN NEW;
      END $$`);
    this.addSql(`CREATE TRIGGER wallets_guard BEFORE INSERT OR UPDATE OR DELETE ON wallets
                 FOR EACH ROW EXECUTE FUNCTION wallets_guard()`);

    // (d) Saldo materializado == último lançamento, verificado no COMMIT (deferred).
    this.addSql(`
      CREATE FUNCTION assert_wallet_ledger_consistent(p_wallet_id uuid) RETURNS void LANGUAGE plpgsql AS $$
      DECLARE w record; l record;
      BEGIN
        -- Relê a linha: com deferred trigger o NEW do evento pode estar defasado.
        SELECT balance, version INTO w FROM wallets WHERE id = p_wallet_id;
        SELECT balance_after, wallet_version INTO l FROM wallet_ledger_entries
         WHERE wallet_id = p_wallet_id ORDER BY wallet_version DESC LIMIT 1;
        IF NOT FOUND THEN
          IF w.balance <> 0 THEN
            RAISE EXCEPTION 'wallet % has balance without ledger', p_wallet_id USING ERRCODE = 'WL004';
          END IF;
        ELSIF w.balance <> l.balance_after OR w.version <> l.wallet_version THEN
          RAISE EXCEPTION 'wallet % diverges from ledger', p_wallet_id USING ERRCODE = 'WL004';
        END IF;
      END $$`);
    this.addSql(`
      CREATE FUNCTION trg_wallets_consistency() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN PERFORM assert_wallet_ledger_consistent(NEW.id); RETURN NULL; END $$`);
    this.addSql(`
      CREATE FUNCTION trg_ledger_consistency() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN PERFORM assert_wallet_ledger_consistent(NEW.wallet_id); RETURN NULL; END $$`);
    this.addSql(`CREATE CONSTRAINT TRIGGER wallets_ledger_consistency AFTER INSERT OR UPDATE ON wallets
                 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION trg_wallets_consistency()`);
    this.addSql(`CREATE CONSTRAINT TRIGGER ledger_wallet_consistency AFTER INSERT ON wallet_ledger_entries
                 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION trg_ledger_consistency()`);

    // (e) Máquina de estados: terminais congelados, campos de negócio imutáveis, sem DELETE.
    this.addSql(`
      CREATE FUNCTION wager_transactions_guard() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF TG_OP = 'DELETE' THEN
          RAISE EXCEPTION 'wager_transactions cannot be deleted' USING ERRCODE = 'WT001';
        END IF;
        IF OLD.status IN ('PROCESSED','REJECTED','FAILED') THEN
          RAISE EXCEPTION 'transaction % is terminal (%)', OLD.id, OLD.status USING ERRCODE = 'WT001';
        END IF;
        IF NOT (OLD.status IN ('PENDING','PENDING_REFERENCE')
                AND NEW.status IN ('PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')) THEN
          RAISE EXCEPTION 'invalid transition % -> %', OLD.status, NEW.status USING ERRCODE = 'WT001';
        END IF;
        IF (NEW.id, NEW.provider_id, NEW.external_transaction_id, NEW.idempotency_key, NEW.payload_hash,
            NEW.wallet_id, NEW.player_id, NEW.round_id, NEW.game_id, NEW.kind, NEW.amount, NEW.currency,
            NEW.reference_external_transaction_id, NEW.created_at)
           IS DISTINCT FROM
           (OLD.id, OLD.provider_id, OLD.external_transaction_id, OLD.idempotency_key, OLD.payload_hash,
            OLD.wallet_id, OLD.player_id, OLD.round_id, OLD.game_id, OLD.kind, OLD.amount, OLD.currency,
            OLD.reference_external_transaction_id, OLD.created_at) THEN
          RAISE EXCEPTION 'business fields of transaction % are immutable', OLD.id USING ERRCODE = 'WT001';
        END IF;
        RETURN NEW;
      END $$`);
    this.addSql(`CREATE TRIGGER wager_transactions_guard BEFORE UPDATE OR DELETE ON wager_transactions
                 FOR EACH ROW EXECUTE FUNCTION wager_transactions_guard()`);
  }

  override down(): void {
    this.addSql('DROP TRIGGER wager_transactions_guard ON wager_transactions');
    this.addSql('DROP FUNCTION wager_transactions_guard()');
    this.addSql('DROP TRIGGER ledger_wallet_consistency ON wallet_ledger_entries');
    this.addSql('DROP TRIGGER wallets_ledger_consistency ON wallets');
    this.addSql('DROP FUNCTION trg_ledger_consistency()');
    this.addSql('DROP FUNCTION trg_wallets_consistency()');
    this.addSql('DROP FUNCTION assert_wallet_ledger_consistent(uuid)');
    this.addSql('DROP TRIGGER wallets_guard ON wallets');
    this.addSql('DROP FUNCTION wallets_guard()');
    this.addSql('DROP TRIGGER ledger_chain ON wallet_ledger_entries');
    this.addSql('DROP FUNCTION ledger_assert_chain()');
    this.addSql('DROP TRIGGER ledger_no_truncate ON wallet_ledger_entries');
    this.addSql('DROP TRIGGER ledger_no_update_delete ON wallet_ledger_entries');
    this.addSql('DROP FUNCTION ledger_forbid_mutation()');
  }
}
