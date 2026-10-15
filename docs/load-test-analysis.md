# Análise do teste de carga

O relatório da última execução (`bun run test:load`) está em [load-test-report.md](load-test-report.md).
Este documento compara as execuções feitas durante o desenvolvimento e registra o que cada uma ensinou.
Todas rodaram na mesma máquina (Ryzen 5 4600G, 12 threads, 15 GB, Windows 11 + Docker Desktop/WSL2),
com 3 réplicas atrás do nginx, pool de 10 conexões por réplica, 50 clientes em malha fechada por 30 s
e 200 wallets.

## Execuções

| # | Cenário | Gerador | Outbox batch | req/s | p50 | p95 | p99 | máx | Erros | Outbox lag máx |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 20% hot wallet | **host** (`localhost:8088`) | 10 | 152.9 | 53 ms | 726 ms | 2.6 s | 7.7 s | **23 rede (0.49%)** | 2.3 s |
| 2 | 20% hot wallet | rede do compose | 10 | 101.2 | 214 ms | 1.8 s | 4.3 s | 12.8 s | 0 | 2.1 s |
| 3 | uniforme (sem hot) | rede do compose | 10 | 236.1 | 179 ms | 453 ms | 802 ms | 1.1 s | 0 | **19.4 s** |
| 4 | uniforme (sem hot) | rede do compose | **100** | **288.1** | 160 ms | 302 ms | **434 ms** | 932 ms | 0 | **0.84 s** |
| 5 | 20% hot wallet (oficial) | rede do compose | 100 | 125.8 | 170 ms | — | 3.5 s | 10.5 s | 0 | 0.83 s |

Execução 5, latência por destino:

| Requisições | p50 | p99 | máx |
|---|---|---|---|
| hot wallet (757) | 488 ms | 7.3 s | 10.5 s |
| demais wallets (3088) | 131 ms | 1.37 s | 2.1 s |

**Consistência final em todas as execuções:**
- 201 wallets verificadas;
- 0 divergências entre saldo e ledger;
- 0 lançamentos duplicados;
- 0 saldos negativos;
- nenhum 5xx.

## Achados

### 1. Os erros de rede eram do ambiente, não da aplicação
Na execução 1, 0,49% das requisições falharam com `ECONNRESET` exatamente aos **30,06 s**, e **nenhuma
delas aparece no log do nginx**. Ou seja, a conexão morreu antes de chegar ao proxy.

A mesma carga rodando **dentro** da rede do compose (container chamando `http://nginx:80`) teve
**0 erros em cerca de 3.400 requisições**, em 3 execuções. A causa é o encaminhamento de portas do
Docker Desktop no Windows.

Ações tomadas:
- **Medição:** o gerador de carga roda como serviço `loadgen` dentro da rede.
- **e2e multi-instância:** continua batendo em `localhost:8088` e trata `ECONNRESET` como um provedor
  real faria, reenviando **com a mesma Idempotency-Key**. A asserção de débito único continua valendo.

Isso explica também a falha isolada observada na Fase 2.

### 2. Outbox com lote 10 não acompanhava a vazão
Na execução 3, a cerca de 236 req/s (mais de 470 eventos/s), o lag chegou a 19 s e não parava de crescer:
cada transação do publisher carregava só 10 eventos.

Com `OUTBOX_BATCH_SIZE=100` (execução 4), o lag máximo caiu para **0,84 s**. A vazão HTTP também subiu
(menos transações curtas disputando o pool), e o p99 caiu de 802 ms para 434 ms.
O valor ficou fixado no compose.

### 3. Hot wallet: o custo da serialização por wallet, e onde ele vaza
Com 20% do tráfego numa única wallet, a vazão cai de 288 para cerca de 126 req/s, e a cauda cresce.

O mecanismo, que é o esperado do desenho escolhido (lock pessimista por wallet, decisão A):
- **Seção crítica serial:** as transações da hot wallet são executadas uma por vez. Com cerca de
  10 clientes nela, cada uma espera a fila inteira; daí o p50 de 488 ms e o p99 de 7,3 s.
- **Transbordo pelo pool:** enquanto esperam o lock, essas transações **seguram conexões do pool**
  (10 por réplica). As requisições de outras wallets passam a esperar conexão, e o p99 delas sobe
  de 434 ms (sem hot wallet) para 1,37 s.
- **Correção não é afetada:** nenhum erro, nenhuma inconsistência, e 0 ou 1 retry por `lock_timeout`
  em toda a execução.

**Mitigações possíveis, não implementadas** (o enunciado prioriza correção, e cada uma tem custo):

| Mitigação | Efeito | Custo |
|---|---|---|
| Fila em memória por walletId dentro de cada instância (single-flight) | no máximo 1 conexão por wallet por instância esperando o lock; o transbordo some | complexidade; não muda a seção crítica |
| Rotear hot wallets pela fila FIFO (`MessageGroupId = walletId`) | serializa sem segurar conexões | latência assíncrona para o provedor |
| `lock_timeout` curto só para o lock da wallet + 503 rápido | protege o pool; o provedor reenvia | mais retries visíveis |
| Pool maior / PgBouncer | adia o transbordo | só desloca o limite |

### 4. SQS
O consumo variou entre 39 e 118 msg/s entre execuções, com 3 consumers e 200 grupos FIFO.
O fator limitante é o próprio protocolo:
- `ReceiveMessage` com no máximo 10 mensagens;
- dentro de um grupo, processamento sequencial (preserva a ordem por wallet);
- um round trip ao LocalStack para cada ack.

A variação acompanha a carga residual do Postgres (publishers e workers) e o próprio LocalStack, que
é um emulador.

## Limites desta medição
- **Mesma máquina:** gerador, proxy, aplicação, banco e emulador disputam CPU. Os números são um piso.
- **LocalStack não é SQS real:** latências e limites de FIFO diferem.
- **Duração:** 30 s por fase mede regime sustentado curto, sem efeitos de longo prazo
  (autovacuum, crescimento de índices, retenção da outbox).
- **Malha fechada:** a vazão se ajusta à latência. Não há medição de latência sob taxa constante
  (malha aberta) nem curva de saturação.
