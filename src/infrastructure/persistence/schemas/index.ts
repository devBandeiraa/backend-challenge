import { InboxRecord, LedgerEntryRecord, OutboxRecord, WagerTransactionRecord, WalletRecord } from './records';

export const entities = [WalletRecord, WagerTransactionRecord, LedgerEntryRecord, OutboxRecord, InboxRecord];
export { InboxRecord, LedgerEntryRecord, OutboxRecord, WagerTransactionRecord, WalletRecord };
