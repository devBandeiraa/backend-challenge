/**
 * Duas identidades no banco (menor privilégio):
 *  - DATABASE_URL: usuário da APLICAÇÃO. Sem superuser, sem ownership; só os GRANTs da migration 008.
 *  - MIGRATION_DATABASE_URL: OWNER do schema. Usado só por migrations (e por testes que precisam
 *    contornar as garantias de propósito, ex.: forçar divergência na reconciliação).
 * Os defaults servem para rodar no host contra o Postgres do compose (porta 5442).
 * Lidas na hora (não no import): testes sobem instâncias com variáveis próprias.
 */
export const appDatabaseUrl = (): string =>
  process.env.DATABASE_URL ?? 'postgresql://wagering_app:wagering_app@localhost:5442/wagering';

export const migrationDatabaseUrl = (): string =>
  process.env.MIGRATION_DATABASE_URL ?? 'postgresql://wagering:wagering@localhost:5442/wagering';
