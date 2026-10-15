import type { MigrationObject } from '@mikro-orm/core';
import { Migration001CreateWallets } from './001-create-wallets';
import { Migration002CreateWagerTransactions } from './002-create-wager-transactions';
import { Migration003CreateWalletLedgerEntries } from './003-create-wallet-ledger-entries';
import { Migration004IntegrityTriggers } from './004-integrity-triggers';
import { Migration005CreateInboxOutbox } from './005-create-inbox-outbox';

// Lista explícita em vez de glob: ordem determinística, funciona igual em dev/teste/imagem
// e não depende de descoberta por filesystem (que varia entre Bun/Node/bundle).
export const migrationsList: MigrationObject[] = [
  { name: '001-create-wallets', class: Migration001CreateWallets },
  { name: '002-create-wager-transactions', class: Migration002CreateWagerTransactions },
  { name: '003-create-wallet-ledger-entries', class: Migration003CreateWalletLedgerEntries },
  { name: '004-integrity-triggers', class: Migration004IntegrityTriggers },
  { name: '005-create-inbox-outbox', class: Migration005CreateInboxOutbox },
];
