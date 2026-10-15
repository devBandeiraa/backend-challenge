import { Migration } from '@mikro-orm/migrations';

export class Migration005CreateInboxOutbox extends Migration {
  override name = '005-create-inbox-outbox';

  override up(): void {
    this.addSql(`
      CREATE TABLE inbox_messages (
        consumer_name   varchar(64)  NOT NULL,
        message_id      varchar(128) NOT NULL,
        payload_hash    char(64)     NOT NULL,
        -- NULL quando a mensagem não gerou transação (ex.: WALLET_NOT_FOUND, decisão D).
        transaction_id  uuid         REFERENCES wager_transactions(id),
        received_at     timestamptz  NOT NULL DEFAULT now(),
        processed_at    timestamptz,
        CONSTRAINT inbox_pk       PRIMARY KEY (consumer_name, message_id),
        CONSTRAINT inbox_hash_chk CHECK (payload_hash ~ '^[0-9a-f]{64}$')
      )`);

    this.addSql(`
      CREATE TABLE outbox_messages (
        id               uuid          PRIMARY KEY,   -- = eventId
        aggregate_type   varchar(64)   NOT NULL,
        aggregate_id     uuid          NOT NULL,
        event_type       varchar(128)  NOT NULL,
        event_version    integer       NOT NULL,
        payload          jsonb         NOT NULL,
        correlation_id   varchar(128)  NOT NULL,
        occurred_at      timestamptz   NOT NULL,
        attempts         integer       NOT NULL DEFAULT 0,
        next_attempt_at  timestamptz   NOT NULL DEFAULT now(),
        published_at     timestamptz,
        last_error       varchar(1024),
        CONSTRAINT outbox_attempts_chk CHECK (attempts >= 0),
        CONSTRAINT outbox_version_chk  CHECK (event_version >= 1)
      )`);
    // Parcial: o índice só contém o backlog, então fica pequeno mesmo com a tabela crescendo.
    this.addSql(`CREATE INDEX outbox_due_idx ON outbox_messages (next_attempt_at) WHERE published_at IS NULL`);
  }

  override down(): void {
    this.addSql('DROP TABLE outbox_messages');
    this.addSql('DROP TABLE inbox_messages');
  }
}
