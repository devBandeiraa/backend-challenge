import { Migration } from '@mikro-orm/migrations';

const BASE_CODES = [
  'INSUFFICIENT_FUNDS',
  'REVERSAL_WOULD_OVERDRAW',
  'REFERENCE_NOT_FOUND',
  'REFERENCE_MISMATCH',
  'REFERENCE_ALREADY_REVERSED',
  'REFERENCE_NOT_PROCESSED',
  'AMOUNT_MISMATCH',
  'CURRENCY_MISMATCH',
  'INVALID_REFERENCE_KIND',
  'PLAYER_MISMATCH',
  'PROCESSING_FAILED',
];

const check = (codes: string[]) =>
  `ALTER TABLE wager_transactions ADD CONSTRAINT wt_failure_code_chk
     CHECK (failure_code IS NULL OR failure_code IN (${codes.map((c) => `'${c}'`).join(',')}))`;

/**
 * BALANCE_LIMIT_EXCEEDED: crédito que levaria o saldo além de NUMERIC(19,2) é rejeição de
 * negócio, não erro interno. A taxonomia do schema acompanha o catálogo do domínio.
 * O down só é possível se nenhuma transação tiver o código novo (o CHECK antigo a recusaria).
 */
export class Migration006BalanceLimitFailureCode extends Migration {
  override name = '006-balance-limit-failure-code';

  override up(): void {
    this.addSql('ALTER TABLE wager_transactions DROP CONSTRAINT wt_failure_code_chk');
    this.addSql(check([...BASE_CODES, 'BALANCE_LIMIT_EXCEEDED']));
  }

  override down(): void {
    this.addSql('ALTER TABLE wager_transactions DROP CONSTRAINT wt_failure_code_chk');
    this.addSql(check(BASE_CODES));
  }
}
