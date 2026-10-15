import { defineEntity, p } from '@mikro-orm/core';

// Registro de persistência, separado do aggregate de domínio (que não conhece o ORM).
// numeric é mapeado como string: o valor nunca passa por number entre o banco e o Money.
export const WalletRecord = defineEntity({
  name: 'WalletRecord',
  tableName: 'wallets',
  properties: {
    id: p.uuid().primary(),
    playerId: p.uuid(),
    currency: p.string().length(3),
    balance: p.decimal('string').precision(19).scale(2),
    version: p.integer(),
    createdAt: p.datetime(),
    updatedAt: p.datetime(),
  },
});
