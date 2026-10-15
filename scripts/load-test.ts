/**
 * bun run test:load: orquestra o teste de carga.
 *  1. sobe o stack completo (3 réplicas + nginx + Postgres + LocalStack);
 *  2. coleta o ambiente (no host);
 *  3. roda o gerador (scripts/load-runner.ts) DENTRO da rede do compose, serviço `loadgen`;
 *  4. grava docs/load-test-report.md e desliga as réplicas (LOAD_KEEP_STACK=1 mantém).
 * Variáveis do gerador: LOAD_DURATION_S, LOAD_CONCURRENCY, LOAD_WALLETS, LOAD_HOT_SHARE,
 * LOAD_REPLAY_SHARE, LOAD_SQS_MESSAGES (ver load-runner.ts).
 */
import { cpus, release, totalmem, type as osType } from 'node:os';

const decode = (b: Uint8Array) => new TextDecoder().decode(b).trim();
const sh = (cmd: string[]) => decode(Bun.spawnSync(cmd).stdout);
const run = (cmd: string[], env: Record<string, string> = {}) =>
  Bun.spawn(cmd, { stdout: 'inherit', stderr: 'inherit', env: { ...process.env, ...env } }).exited;
const round = (n: number, d = 1) => Math.round(n * 10 ** d) / 10 ** d;

const LOAD_VARS = ['LOAD_DURATION_S', 'LOAD_CONCURRENCY', 'LOAD_WALLETS', 'LOAD_HOT_SHARE', 'LOAD_REPLAY_SHARE', 'LOAD_SQS_MESSAGES'];

let code = await run(['docker', 'compose', 'up', '-d', '--build', '--wait']);
if (code === 0) {
  const environment = {
    'Sistema operacional (host)': `${osType()} ${release()}`,
    'CPU (host)': `${cpus().length} threads · ${cpus()[0]?.model.trim()}`,
    'Memória (host)': `${round(totalmem() / 1024 ** 3)} GB`,
    'Docker': sh(['docker', 'info', '--format', '{{.ServerVersion}} · {{.OperatingSystem}} · {{.NCPU}} CPUs · {{.MemTotal}} bytes']),
    'Bun': Bun.version,
    'PostgreSQL': sh(['docker', 'compose', 'exec', '-T', 'postgres', 'postgres', '--version']),
    'Réplicas da app': String(sh(['docker', 'compose', 'ps', '-q', 'app']).split('\n').filter(Boolean).length),
    'Pool por réplica': '10 conexões',
  };

  const loadArgs = LOAD_VARS.filter((k) => process.env[k]).flatMap((k) => ['-e', `${k}=${process.env[k]}`]);
  const proc = Bun.spawn(
    ['docker', 'compose', 'run', '--rm', '--build', ...loadArgs, 'loadgen'],
    { stdout: 'pipe', stderr: 'inherit', env: { ...process.env, COMPOSE_PROFILES: `${process.env.COMPOSE_PROFILES ?? 'localstack'},load` } },
  );
  const output = await new Response(proc.stdout).text();
  code = await proc.exited;
  const line = output.split('\n').find((l) => l.startsWith('RESULTS_JSON:'));
  if (code === 0 && line) {
    const results = JSON.parse(line.slice('RESULTS_JSON:'.length));
    console.log(JSON.stringify(results, null, 2));
    await Bun.write('docs/load-test-report.md', report(environment, results));
    console.log('report written to docs/load-test-report.md');
    const c = results.consistency;
    if (c.ledgerMismatches || c.duplicatedLedgerEntries || c.negativeBalances) code = 1;
  } else {
    console.error(output);
    code = code || 1;
  }
}
if (process.env.LOAD_KEEP_STACK !== '1') await run(['docker', 'compose', 'stop', 'app', 'nginx']);
process.exit(code);

function report(environment: Record<string, string>, r: any): string {
  const c = r.config;
  return `# Relatório do teste de carga

> Gerado por \`bun run test:load\` em ${new Date().toISOString()}. Os números dependem da máquina;
> o que vale é a metodologia e a consistência final. Para reproduzir: \`bun run test:load\`.

## Ambiente
| Item | Valor |
|---|---|
${Object.entries(environment).map(([k, v]) => `| ${k} | ${v} |`).join('\n')}

Tudo roda na mesma máquina: gerador de carga, nginx, 3 réplicas, Postgres e LocalStack em Docker Desktop (WSL2).
Gerador, servidor e banco disputam a mesma CPU, então os números são um **piso**, não uma capacidade de produção.

## Metodologia
- **Gerador dentro da rede do compose** (serviço \`loadgen\`), chamando \`http://nginx:80\`.
  - Pelo host, o encaminhamento de portas do Docker Desktop no Windows derruba cerca de 0,4% das conexões
    com \`ECONNRESET\` aos 30 s, sem que a requisição chegue ao nginx.
  - Isso é artefato do ambiente, não da aplicação: há evidência no ARCHITECTURE.md, e a mesma carga sem
    esse salto teve 0 erros. Medir de dentro da rede remove esse ruído.
- **Alvo:** nginx distribuindo entre 3 réplicas. Cada uma tem pool de 10 conexões, consumer SQS,
  publisher da outbox e worker de referências.
- **Fase HTTP (malha fechada):** ${c.concurrency} clientes concorrentes durante ${c.durationS} s. Cada cliente só envia
  a próxima requisição depois de receber a resposta: sem taxa-alvo, mede-se a vazão sustentada.
  - Mistura: 80% BET 1.00, 15% WIN 2.00, 5% LOSS.
  - **${Math.round(c.hotShare * 100)}% do tráfego numa hot wallet**, para contenção de lock; o resto
    uniformemente em ${c.wallets} wallets.
  - ${Math.round(c.replayShare * 100)}% de reenvios idênticos (mesma Idempotency-Key) de requisições anteriores.
  - Latência medida no gerador: do envio até a resposta completa, passando pelo nginx.
- **Fase SQS:** ${c.sqsMessages} BETs enfileiradas (MessageGroupId = walletId). Mede-se o tempo até todas
  estarem PROCESSED no banco, com os 3 consumers competindo.
- **Outbox lag:** idade do evento pendente mais antigo, amostrada a cada 1 s pelo relógio do banco durante
  as duas fases, mais o tempo para escoar depois da carga.
  - Backlog anterior (${r.outboxBacklogBeforeRun} eventos) foi escoado antes de medir.
- **Conflitos:** variação de \`wagering_db_lock_conflicts_total\` (um incremento por retry) somada nas 3 réplicas.
- **Consistência final:** para todas as wallets da carga, saldo = soma do ledger e version = último
  lançamento; nenhum lançamento duplicado; nenhum saldo negativo.

## Resultados
### HTTP
| Métrica | Valor |
|---|---|
| Requisições | ${r.http.requests} em ${r.http.durationS} s |
| Throughput | **${r.http.throughputRps} req/s** |
| Latência p50 / p95 / p99 / máx | **${r.http.latencyMs.p50} / ${r.http.latencyMs.p95} / ${r.http.latencyMs.p99}** / ${r.http.latencyMs.max} ms |
| ↳ só hot wallet (${r.http.latencyByTarget.hotWallet.count} req) | ${r.http.latencyByTarget.hotWallet.p50} / ${r.http.latencyByTarget.hotWallet.p95} / ${r.http.latencyByTarget.hotWallet.p99} / ${r.http.latencyByTarget.hotWallet.max} ms |
| ↳ demais wallets (${r.http.latencyByTarget.otherWallets.count} req) | ${r.http.latencyByTarget.otherWallets.p50} / ${r.http.latencyByTarget.otherWallets.p95} / ${r.http.latencyByTarget.otherWallets.p99} / ${r.http.latencyByTarget.otherWallets.max} ms |
| Status | ${Object.entries(r.http.byStatus).map(([k, v]) => `${k}: ${v}`).join(' · ')} |
| Replays idempotentes | ${r.http.replays} |
| Rejeições de negócio (422) | ${Object.keys(r.http.businessRejections).length ? JSON.stringify(r.http.businessRejections) : 'nenhuma'} |
| Erros (5xx + rede) | ${r.http.errors.http5xx} + ${r.http.errors.network} (**${r.http.errors.rate}%**) |

### SQS
| Métrica | Valor |
|---|---|
| Mensagens | ${r.sqs.messages} (enfileiradas em ${r.sqs.enqueueS} s) |
| Tempo até todas processadas | ${r.sqs.endToEndS} s |
| Throughput de consumo | **${r.sqs.throughputMsgPerS} msg/s** |

### Concorrência e outbox
| Métrica | Valor |
|---|---|
| Retries por conflito de lock | ${r.concurrency.lockConflictRetries} |
| Unidades de trabalho que desistiram (503) | ${r.concurrency.retriesExhausted} |
| Outbox lag máximo / p95 | ${r.outbox.maxLagS} s / ${r.outbox.p95LagS} s |
| Escoamento da outbox após a carga | ${r.outbox.drainAfterLoadS} s |

### Consistência final
| Verificação | Resultado |
|---|---|
| Wallets verificadas | ${r.consistency.walletsChecked} |
| Saldo ≠ ledger ou version ≠ último lançamento | **${r.consistency.ledgerMismatches}** |
| Transações com mais de um lançamento | **${r.consistency.duplicatedLedgerEntries}** |
| Saldos negativos | **${r.consistency.negativeBalances}** |
`;
}
