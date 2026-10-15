import { afterEach, beforeEach } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import { assertLedgerInvariant } from './app-context';

/**
 * Registra um afterEach que verifica o invariante nas wallets tocadas pelo teste CORRENTE (§13: invariante
 * final de todos os testes). Devolve `touch(id)` para marcar cada wallet criada/movimentada pelo teste.
 */
export function ledgerInvariantAfterEach(getOrm: () => MikroORM): (walletId: string) => string {
  const touched = new Set<string>();
  afterEach(async () => {
    const ids = [...touched];
    touched.clear();
    for (const id of ids) await assertLedgerInvariant(getOrm(), id);
  });
  return (walletId) => {
    touched.add(walletId);
    return walletId;
  };
}

/**
 * Variante para testes de SQL puro (sem a aplicação): ao fim de cada teste, verifica o invariante em TODA
 * wallet inserida ou alterada desde o início dele. Critério por xid (xmin da linha > xid capturado no
 * beforeEach), não por relógio: o relógio do host e o da VM do Docker divergem, e um critério por tempo
 * pegaria wallets de outros arquivos (inclusive as corrompidas de propósito na reconciliação).
 * Assume que o contador de xid não deu a volta (wraparound) durante a execução, o que vale para testes.
 */
export function ledgerInvariantSinceTestStart(getOrm: () => MikroORM): void {
  let sinceXid = 0;
  beforeEach(async () => {
    const [row] = await getOrm().em.fork().execute<{ xid: string }[]>(`SELECT (txid_current() % 4294967296)::text AS xid`);
    sinceXid = Number(row!.xid);
  });
  afterEach(async () => {
    const rows = await getOrm()
      .em.fork()
      .execute<{ id: string }[]>(`SELECT id FROM wallets WHERE xmin::text::bigint > ?`, [sinceXid]);
    for (const { id } of rows) await assertLedgerInvariant(getOrm(), id);
  });
}
