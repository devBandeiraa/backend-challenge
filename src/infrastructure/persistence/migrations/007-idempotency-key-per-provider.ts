import { Migration } from '@mikro-orm/migrations';

/**
 * Idempotency-Key com escopo por provedor: a key é escolhida pelo provedor, e dois provedores
 * podem gerar a mesma (contadores, ids de rodada). Com unicidade global, a operação legítima de
 * um seria bloqueada (409) pela do outro. O replay passa a ser resolvido por (provider_id, key).
 * O down só é possível se nenhuma key se repetir entre provedores (o UNIQUE global a recusaria).
 */
export class Migration007IdempotencyKeyPerProvider extends Migration {
  override name = '007-idempotency-key-per-provider';

  override up(): void {
    this.addSql('ALTER TABLE wager_transactions DROP CONSTRAINT wt_idempotency_key_uq');
    this.addSql(
      'ALTER TABLE wager_transactions ADD CONSTRAINT wt_provider_idempotency_key_uq UNIQUE (provider_id, idempotency_key)',
    );
  }

  override down(): void {
    this.addSql('ALTER TABLE wager_transactions DROP CONSTRAINT wt_provider_idempotency_key_uq');
    this.addSql('ALTER TABLE wager_transactions ADD CONSTRAINT wt_idempotency_key_uq UNIQUE (idempotency_key)');
  }
}
