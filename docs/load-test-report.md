# Relatório do teste de carga

> Gerado por `bun run test:load` em 2026-10-15T17:05:49.857Z. Os números dependem da máquina;
> o que vale é a metodologia e a consistência final. Para reproduzir: `bun run test:load`.

## Ambiente
| Item | Valor |
|---|---|
| Sistema operacional (host) | Windows_NT 10.0.26200 |
| CPU (host) | 12 threads · AMD Ryzen 5 4600G with Radeon Graphics |
| Memória (host) | 15.4 GB |
| Docker | 29.6.1 · Docker Desktop · 12 CPUs · 8003403776 bytes |
| Bun | 1.4.2 |
| PostgreSQL | postgres (PostgreSQL) 17.11 |
| Réplicas da app | 3 |
| Pool por réplica | 10 conexões |

Tudo roda na mesma máquina: gerador de carga, nginx, 3 réplicas, Postgres e LocalStack em Docker Desktop (WSL2).
Gerador, servidor e banco disputam a mesma CPU, então os números são um **piso**, não uma capacidade de produção.

## Metodologia
- **Gerador dentro da rede do compose** (serviço `loadgen`), chamando `http://nginx:80`.
  - Pelo host, o encaminhamento de portas do Docker Desktop no Windows derruba cerca de 0,4% das conexões
    com `ECONNRESET` aos 30 s, sem que a requisição chegue ao nginx.
  - Isso é artefato do ambiente, não da aplicação: há evidência no ARCHITECTURE.md, e a mesma carga sem
    esse salto teve 0 erros. Medir de dentro da rede remove esse ruído.
- **Alvo:** nginx distribuindo entre 3 réplicas. Cada uma tem pool de 10 conexões, consumer SQS,
  publisher da outbox e worker de referências.
- **Fase HTTP (malha fechada):** 50 clientes concorrentes durante 30 s. Cada cliente só envia
  a próxima requisição depois de receber a resposta: sem taxa-alvo, mede-se a vazão sustentada.
  - Mistura: 80% BET 1.00, 15% WIN 2.00, 5% LOSS.
  - **20% do tráfego numa hot wallet**, para contenção de lock; o resto
    uniformemente em 200 wallets.
  - 5% de reenvios idênticos (mesma Idempotency-Key) de requisições anteriores.
  - Latência medida no gerador: do envio até a resposta completa, passando pelo nginx.
- **Fase SQS:** 2000 BETs enfileiradas (MessageGroupId = walletId). Mede-se o tempo até todas
  estarem PROCESSED no banco, com os 3 consumers competindo.
- **Outbox lag:** idade do evento pendente mais antigo, amostrada a cada 1 s pelo relógio do banco durante
  as duas fases, mais o tempo para escoar depois da carga.
  - Backlog anterior (32 eventos) foi escoado antes de medir.
- **Conflitos:** variação de `wagering_db_lock_conflicts_total` (um incremento por retry) somada nas 3 réplicas.
- **Consistência final:** para todas as wallets da carga, saldo = soma do ledger e version = último
  lançamento; nenhum lançamento duplicado; nenhum saldo negativo.

## Resultados
### HTTP
| Métrica | Valor |
|---|---|
| Requisições | 3845 em 30.6 s |
| Throughput | **125.8 req/s** |
| Latência p50 / p95 / p99 / máx | **170.3 / 1413.9 / 3522.9** / 10530.8 ms |
| ↳ só hot wallet (757 req) | 488 / 3553.7 / 7277.9 / 10530.8 ms |
| ↳ demais wallets (3088 req) | 130.8 / 869 / 1373.4 / 2129.5 ms |
| Status | 200: 3845 |
| Replays idempotentes | 188 |
| Rejeições de negócio (422) | nenhuma |
| Erros (5xx + rede) | 0 + 0 (**0%**) |

### SQS
| Métrica | Valor |
|---|---|
| Mensagens | 2000 (enfileiradas em 1.8 s) |
| Tempo até todas processadas | 50.7 s |
| Throughput de consumo | **39.4 msg/s** |

### Concorrência e outbox
| Métrica | Valor |
|---|---|
| Retries por conflito de lock | 0 |
| Unidades de trabalho que desistiram (503) | 0 |
| Outbox lag máximo / p95 | 0.83 s / 0.34 s |
| Escoamento da outbox após a carga | 0 s |

### Consistência final
| Verificação | Resultado |
|---|---|
| Wallets verificadas | 201 |
| Saldo ≠ ledger ou version ≠ último lançamento | **0** |
| Transações com mais de um lançamento | **0** |
| Saldos negativos | **0** |
