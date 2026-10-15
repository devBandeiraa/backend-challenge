import type { MigrationObject } from '@mikro-orm/core';
import { Migration001CreateWallets } from './001-create-wallets';
import { Migration002CreateWagerTransactions } from './002-create-wager-transactions';
import { Migration003CreateWalletLedgerEntries } from './003-create-wallet-ledger-entries';
import { Migration004IntegrityTriggers } from './004-integrity-triggers';
import { Migration005CreateInboxOutbox } from './005-create-inbox-outbox';
import { Migration006BalanceLimitFailureCode } from './006-balance-limit-failure-code';
import { Migration007IdempotencyKeyPerProvider } from './007-idempotency-key-per-provider';
import { Migration008AppRoleLeastPrivilege } from './008-app-role-least-privilege';
import { Migration009LedgerMatchesTransaction } from './009-ledger-matches-transaction';

// Lista explícita em vez de glob: ordem determinística, funciona igual em dev/teste/imagem
// e não depende de descoberta por filesystem (que varia entre Bun/Node/bundle).
export const migrationsList: MigrationObject[] = [
  { name: '001-create-wallets', class: Migration001CreateWallets },
  { name: '002-create-wager-transactions', class: Migration002CreateWagerTransactions },
  { name: '003-create-wallet-ledger-entries', class: Migration003CreateWalletLedgerEntries },
  { name: '004-integrity-triggers', class: Migration004IntegrityTriggers },
  { name: '005-create-inbox-outbox', class: Migration005CreateInboxOutbox },
  { name: '006-balance-limit-failure-code', class: Migration006BalanceLimitFailureCode },
  { name: '007-idempotency-key-per-provider', class: Migration007IdempotencyKeyPerProvider },
  { name: '008-app-role-least-privilege', class: Migration008AppRoleLeastPrivilege },
  { name: '009-ledger-matches-transaction', class: Migration009LedgerMatchesTransaction },
];
