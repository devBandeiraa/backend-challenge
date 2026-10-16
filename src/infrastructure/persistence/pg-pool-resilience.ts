// Tipos mínimos do `pg` (dependência transitiva do driver do MikroORM, sem @types/pg).
interface PoolClient {
  readonly _queryable?: boolean;
  on(event: 'error', listener: (error: Error) => void): unknown;
  query: (...args: unknown[]) => unknown;
}
interface Pool {
  on(event: 'error', listener: (error: Error) => void): unknown;
  on(event: 'connect', listener: (client: PoolClient) => void): unknown;
}

/**
 * Proteções do pool `pg` contra queda do Postgres (auditoria v1.0.0, achado A1).
 *
 * Sem elas, uma query enviada pouco antes de o servidor sumir esperava a resposta para sempre: o socket
 * não recebe FIN/RST quando o container morre, não há dado a retransmitir e o `statement_timeout` é do
 * servidor. O cliente ficava "emprestado" para sempre, o pool enchia e a réplica travava (readiness,
 * consumer e workers) até ser reiniciada, mesmo com o banco de volta.
 *
 *  - connectionTimeoutMillis: teto para abrir conexão e para esperar uma vaga no pool (falha rápida,
 *    vira 503 / retry SQS em vez de pendurar);
 *  - keepAlive: o kernel sonda a conexão (no compose, sysctls de keepalive curtos): é o que detecta servidor
 *    morto embaixo da única instrução isenta do watchdog, o lock da wallet (LOCK_WAIT_MARKER);
 *  - watchdog de query: query sem resposta além de `queryTimeoutMs` DESTRÓI a conexão. Assim a query
 *    rejeita com erro de conexão (transitório) e o pool descarta o cliente, porque ele deixa de ser
 *    "queryable". O `query_timeout` nativo do `pg` não serve: ele rejeita a query mas mantém o cliente
 *    com a query ativa presa, e o pool o reaproveitaria;
 *  - ROLLBACK/BEGIN em cliente já quebrado viram no-op bem-sucedido: o Kysely só devolve a conexão ao pool
 *    DEPOIS de um ROLLBACK/COMMIT bem-sucedido (ControlledTransaction.rollback) e não a devolve se o BEGIN
 *    falhar. Num cliente cuja conexão caiu, esses comandos falham ("not queryable") e cada falha VAZAVA uma
 *    vaga do pool: com 10 vazamentos a réplica ficava travada para sempre, mesmo com o banco de volta (causa
 *    raiz do achado A1). O no-op é seguro: o servidor já desfez a transação quando a conexão caiu, e o pool
 *    descarta o cliente no release porque ele não é "queryable". COMMIT nunca é simulado;
 *  - handler de 'error': clientes ociosos que caem são registrados (log + métrica) e descartados pelo
 *    próprio pool; sem listener, o evento derrubaria o processo.
 */
export interface PoolResilienceOptions {
  connectTimeoutMs: number;
  /** 0 desliga o watchdog (migrations: DDL longa é legítima). */
  queryTimeoutMs: number;
  applicationName: string;
  onConnectionError?: (error: Error, state: 'idle' | 'active') => void;
}

export function resilientDriverOptions(options: PoolResilienceOptions) {
  return {
    connectionTimeoutMillis: options.connectTimeoutMs,
    keepAlive: true,
    keepAliveInitialDelayMillis: 10_000,
    // Conexões ociosas além disso são fechadas: depois de uma queda, o pool não guarda sockets velhos.
    idleTimeoutMillis: 30_000,
    application_name: options.applicationName,
    onPoolCreated(pool: Pool) {
      pool.on('error', (error) => options.onConnectionError?.(error, 'idle'));
      pool.on('connect', (client: PoolClient) => {
        client.on('error', (error) => options.onConnectionError?.(error, 'active'));
        guardQueries(client, options.queryTimeoutMs);
      });
    },
  };
}

/**
 * Marca a instrução do lock da wallet: ela é isenta do watchdog (e do statement_timeout), porque a fila de
 * uma hot wallet é espera legítima no servidor vivo. Servidor morto embaixo dela é detectado pelo keepalive.
 */
export const LOCK_WAIT_MARKER = '/* wagering:wallet-lock-wait */';

const sqlOf = (arg: unknown): string => (typeof arg === 'string' ? arg : typeof (arg as { text?: unknown })?.text === 'string' ? (arg as { text: string }).text : '');

type Destroyable = { connection?: { stream?: { destroy(error?: Error): void } } };

/** Comandos que, num cliente quebrado, viram no-op (nunca COMMIT). */
const VACUOUS_ON_BROKEN = /^\s*(rollback|begin|start transaction)\b/i;

/**
 * Envolve client.query: ROLLBACK/BEGIN em cliente quebrado viram no-op; com `timeoutMs` > 0, query sem resposta
 * nesse prazo destrói a conexão (exceto a instrução marcada com LOCK_WAIT_MARKER).
 */
export function guardQueries(client: PoolClient, timeoutMs: number): void {
  const query = client.query.bind(client);
  client.query = (...args: unknown[]) => {
    const sql = sqlOf(args[0]);
    if (VACUOUS_ON_BROKEN.test(sql)) {
      const vacuous = { command: sql.trim().split(/\s+/)[0]!.toUpperCase(), rowCount: 0, rows: [], fields: [] };
      if (client._queryable === false) return Promise.resolve(vacuous);
      // Enviado com o cliente ainda são, mas a conexão caiu antes da resposta: mesmo caso, mesmo no-op.
      return (query(...args) as Promise<unknown>).catch((error: unknown) => {
        if (client._queryable === false) return vacuous;
        throw error;
      });
    }
    const result = query(...args);
    if (timeoutMs <= 0 || sql.includes(LOCK_WAIT_MARKER)) return result;
    if (result && typeof (result as Promise<unknown>).then === 'function') {
      const timer = setTimeout(() => {
        (client as unknown as Destroyable).connection?.stream?.destroy(
          new Error(`query exceeded ${timeoutMs} ms without a response: connection destroyed`),
        );
      }, timeoutMs);
      const clear = () => clearTimeout(timer);
      (result as Promise<unknown>).then(clear, clear);
    }
    return result;
  };
}
