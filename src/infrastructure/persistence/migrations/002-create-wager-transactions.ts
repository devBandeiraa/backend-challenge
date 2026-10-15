import { Migration } from '@mikro-orm/migrations';

export class Migration002CreateWagerTransactions extends Migration {
  override name = '002-create-wager-transactions';

  override up(): void {
    this.addSql(`
      CREATE TABLE wager_transactions (
        id                                 uuid          PRIMARY KEY,
        provider_id                        varchar(64)   NOT NULL,
        external_transaction_id            varchar(128)  NOT NULL,
        idempotency_key                    varchar(255)  NOT NULL,
        payload_hash                       char(64)      NOT NULL,
        wallet_id                          uuid          NOT NULL REFERENCES wallets(id),
        -- Sem FK para o player: PLAYER_MISMATCH precisa ser persistido para auditoria/replay.
        player_id                          uuid          NOT NULL,
        round_id                           varchar(128),
        game_id                            varchar(128),
        kind                               varchar(16)   NOT NULL,
        amount                             numeric(19,2) NOT NULL,
        -- Moeda da operação; pode divergir da wallet (rejeitada com CURRENCY_MISMATCH).
        currency                           char(3)       NOT NULL,
        reference_external_transaction_id  varchar(128),
        reference_transaction_id           uuid,
        status                             varchar(24)   NOT NULL,
        failure_code                       varchar(48),
        -- Snapshot do saldo no momento em que a transação ficou terminal: o replay devolve
        -- "o saldo observado naquele momento" mesmo para LOSS/REJECTED, que não têm ledger.
        result_balance                     numeric(19,2),
        result_balance_currency            char(3),
        result_wallet_version              integer,
        attempts                           integer       NOT NULL DEFAULT 0,
        next_attempt_at                    timestamptz,
        correlation_id                     varchar(128)  NOT NULL,
        created_at                         timestamptz   NOT NULL DEFAULT now(),
        updated_at                         timestamptz   NOT NULL DEFAULT now(),
        processed_at                       timestamptz,

        CONSTRAINT wt_idempotency_key_uq       UNIQUE (idempotency_key),
        CONSTRAINT wt_provider_external_uq     UNIQUE (provider_id, external_transaction_id),
        CONSTRAINT wt_id_wallet_uq             UNIQUE (id, wallet_id),
        -- Referência resolvida obrigatoriamente na MESMA wallet (garantia do schema, não só do código).
        CONSTRAINT wt_reference_same_wallet_fk FOREIGN KEY (reference_transaction_id, wallet_id)
                                               REFERENCES wager_transactions(id, wallet_id),
        CONSTRAINT wt_reference_not_self_chk   CHECK (reference_transaction_id IS DISTINCT FROM id),

        CONSTRAINT wt_kind_chk     CHECK (kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
        CONSTRAINT wt_status_chk   CHECK (status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        CONSTRAINT wt_currency_chk CHECK (currency ~ '^[A-Z]{3}$'),
        CONSTRAINT wt_hash_chk     CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
        CONSTRAINT wt_amount_chk   CHECK (amount > 0 OR (kind = 'LOSS' AND amount = 0)),
        CONSTRAINT wt_attempts_chk CHECK (attempts >= 0),

        CONSTRAINT wt_reference_required_chk  CHECK (kind NOT IN ('REFUND','ROLLBACK') OR reference_external_transaction_id IS NOT NULL),
        CONSTRAINT wt_reference_forbidden_chk CHECK (kind NOT IN ('OPENING','BET') OR reference_external_transaction_id IS NULL),
        CONSTRAINT wt_round_game_chk          CHECK (kind = 'OPENING' OR (round_id IS NOT NULL AND game_id IS NOT NULL)),

        CONSTRAINT wt_failure_code_presence_chk CHECK ((status IN ('REJECTED','FAILED')) = (failure_code IS NOT NULL)),
        CONSTRAINT wt_failure_code_chk CHECK (failure_code IS NULL OR failure_code IN (
          'INSUFFICIENT_FUNDS','REVERSAL_WOULD_OVERDRAW','REFERENCE_NOT_FOUND','REFERENCE_MISMATCH',
          'REFERENCE_ALREADY_REVERSED','REFERENCE_NOT_PROCESSED','AMOUNT_MISMATCH','CURRENCY_MISMATCH',
          'INVALID_REFERENCE_KIND','PLAYER_MISMATCH','PROCESSING_FAILED')),
        CONSTRAINT wt_processed_at_chk      CHECK ((status = 'PROCESSED') = (processed_at IS NOT NULL)),
        CONSTRAINT wt_reversal_resolved_chk CHECK (status <> 'PROCESSED' OR kind NOT IN ('REFUND','ROLLBACK')
                                                   OR reference_transaction_id IS NOT NULL),
        CONSTRAINT wt_result_snapshot_chk   CHECK ((status IN ('PROCESSED','REJECTED')) =
                                                   (result_balance IS NOT NULL AND result_balance_currency IS NOT NULL
                                                    AND result_wallet_version IS NOT NULL)),
        CONSTRAINT wt_pending_ref_sched_chk CHECK (status <> 'PENDING_REFERENCE' OR next_attempt_at IS NOT NULL)
      )`);

    this.addSql(`CREATE UNIQUE INDEX wt_one_opening_per_wallet_uq ON wager_transactions (wallet_id) WHERE kind = 'OPENING'`);
    // Decisão B: no máximo UMA reversão efetiva por referência, de qualquer kind.
    // Por kind permitiria REFUND(BET) + ROLLBACK(BET) = crédito duplo.
    this.addSql(`CREATE UNIQUE INDEX wt_single_reversal_uq ON wager_transactions (reference_transaction_id)
                 WHERE status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK')`);
    this.addSql(`CREATE INDEX wt_pending_reference_due_idx ON wager_transactions (next_attempt_at)
                 WHERE status = 'PENDING_REFERENCE'`);
  }

  override down(): void {
    this.addSql('DROP TABLE wager_transactions');
  }
}
