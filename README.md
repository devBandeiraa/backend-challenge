# Distributed Wagering Processor

Serviço financeiro que processa transações de apostas (BET, WIN, LOSS, REFUND, ROLLBACK) vindas de
múltiplos provedores por **HTTP** e **SQS**. É correto com entregas duplicadas, fora de ordem e
concorrentes, inclusive com **3 ou mais instâncias** ao mesmo tempo.

Stack: Bun · TypeScript strict · NestJS · PostgreSQL + MikroORM · SQS (LocalStack) · Docker Compose.

- **Decisões, trade-offs, limitações e a tabela requisito → prova:** [ARCHITECTURE.md](ARCHITECTURE.md)
- Enunciado: [docs/CHALLENGE.md](docs/CHALLENGE.md)
- Teste de carga: [docs/load-test-report.md](docs/load-test-report.md) · [análise](docs/load-test-analysis.md)

## Requisitos

- **Bun 1.4.2**: `powershell -c "irm bun.sh/install.ps1 | iex"` (Windows) ou `curl -fsSL https://bun.sh/install | bash`
- **Docker** com Compose v2.20+ (usa profiles e `depends_on.required`)
- Portas livres: **5442** (Postgres), **4566** (SQS) e **8088** (nginx)

## Início rápido

```bash
bun install
bun run infra:up        # Postgres + emulador SQS + migrations + filas
bun run test            # unidade + integração + concorrência + recuperação (~1–2 min)
bun run test:multi      # ≥ 3 instâncias reais atrás do nginx (sobe e desliga as réplicas)
```

Para usar a API: `bun run stack:up` sobe 3 réplicas atrás do nginx em **http://localhost:8088**.

## Comandos

| Comando | O que faz |
|---|---|
| `bun run infra:up` | Postgres, emulador SQS, migrations e filas (para rodar a app ou os testes no host) |
| `bun run stack:up` | stack completa: 3 réplicas da app + nginx em `localhost:8088` |
| `bun run infra:down` | derruba tudo e **apaga os volumes** |
| `bun run dev` | uma instância local em `localhost:3000`, com reload (precisa de `infra:up`) |
| `bun run migration:up` / `migration:down` / `migration:reset` | migrations versionadas e reversíveis |
| `bun run typecheck` | `tsc --noEmit` (strict) |
| `bun run test` | todos os testes do host (unidade, integração, concorrência, recuperação) |
| `bun run test:unit` · `test:integration` · `test:concurrency` · `test:recovery` | suítes separadas |
| **`bun run test:multi`** | **teste com ≥ 3 instâncias**: sobe o compose (3 réplicas + nginx), roda [multi-instance.e2e.ts](test/e2e/multi-instance.e2e.ts) e desliga as réplicas |
| **`bun run test:all`** | `test` + `test:multi` |
| `bun run test:load` | teste de carga; grava [docs/load-test-report.md](docs/load-test-report.md) |

## Testes

Todos os testes usam **Postgres e LocalStack reais**: não há mock de banco nem de fila.

- **≥ 3 instâncias:** o teste roda via **`bun run test:multi`** (ou `test:all`), e não no `bun run test`,
  porque depende do stack completo. Ele comprova, pelo header `X-Instance-Id`, que as requisições passaram
  pelas 3 réplicas. Cobre a mesma aposta 50× em paralelo, o cenário 100/80/80, os 3 consumers SQS
  concorrentes e o SIGTERM numa réplica durante o consumo.
- **Use `bun run test`, não `bun test` puro:** o script passa `--timeout 60000`, necessário para long
  polling e backoff.
- **Réplicas no ar:** `bun run test` recusa rodar com as réplicas do compose rodando, porque os publishers
  e workers delas disputariam as linhas que os testes medem. O `test:multi` desliga as réplicas ao terminar;
  manualmente: `docker compose stop app nginx`.

## API

Autenticação não implementada: ponto de extensão e desenho no [ARCHITECTURE.md §11](ARCHITECTURE.md#11-autenticação-não-implementada-desenho).

```bash
# criar wallet (saldo inicial > 0 gera OPENING + lançamento no ledger, na mesma transação)
curl -s -X POST localhost:8088/wallets -H 'content-type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1000.00","currency":"BRL"}}'

# submeter transação (Idempotency-Key obrigatória; amount com exatamente 2 casas)
curl -s -X POST localhost:8088/wagering/transactions -H 'content-type: application/json' \
  -H 'Idempotency-Key: provider-a:transaction-123' \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-123","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"<id>","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
```

| Endpoint | Descrição |
|---|---|
| `POST /wallets` | cria wallet (409 se já existir para player + moeda) |
| `GET /wallets/:walletId` | saldo e versão |
| `GET /wallets/:walletId/ledger?cursor=&limit=` | lançamentos, mais recentes primeiro; cursor opaco e estável |
| `POST /wagering/transactions` | submete BET/WIN/LOSS/REFUND/ROLLBACK |
| `GET /wagering/transactions/:transactionId` | transação por id interno |
| `GET /providers/:providerId/wagering/transactions/:externalTransactionId` | transação pelo id do provedor |
| `POST /wallets/:walletId/reconciliation` | compara saldo × ledger; **não corrige**, sinaliza `consistent: false` |
| `GET /health/live` · `GET /health/ready` | liveness · readiness (Postgres + SQS); sem autenticação |
| `GET /metrics` | métricas Prometheus da instância |

**Status:**
- **200:** PROCESSED, também no replay; o campo `idempotentReplay` distingue.
- **201:** wallet criada.
- **202:** PENDING_REFERENCE.
- **400:** payload inválido.
- **404:** recurso consultado não existe.
- **409:** conflito de idempotência ou wallet duplicada.
- **422:** rejeição de negócio, com `failureCode`.
- **503:** falha transitória, com `Retry-After`; reenvie com a **mesma** key.

O replay devolve o mesmo status e o mesmo corpo da primeira resposta. A tabela de `failureCode`
está no [ARCHITECTURE.md §4.4](ARCHITECTURE.md#44-taxonomia-de-failurecode).

### SQS

Fila `wager-transactions.fifo`, com **`MessageGroupId = walletId`**. O envelope é tolerante (só `data` é
obrigatório) e o `data` é estrito:

```json
{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-07-29T15:00:00.000Z",
  "data": {
    "providerId": "provider-a", "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "…", "walletId": "…", "roundId": "round-987", "gameId": "fortune-chimp",
    "kind": "BET", "money": { "amount": "25.00", "currency": "BRL" }
  }
}
```

- **Eventos de integração:** publicados em `wager-events.fifo`.
- **Mensagens com falha permanente ou que esgotaram as tentativas:** vão para `wager-transactions-dlq.fifo`,
  com o motivo no atributo `failureReason`.

## Configuração

Lida na subida da instância; os defaults servem para rodar no host.

| Variável | Padrão | Descrição |
|---|---|---|
| `DATABASE_URL` | `postgresql://wagering:wagering@localhost:5442/wagering` | Postgres |
| `DB_POOL_MAX` | 10 | conexões por instância |
| `DB_LOCK_TIMEOUT_MS` | 5000 | teto de espera por lock em cada transação |
| `UOW_MAX_ATTEMPTS` | 5 | tentativas da transação em conflito de concorrência |
| `APP_ROLES` | `http,consumer,outbox,pending-reference` | papéis desta instância |
| `SQS_ENDPOINT` | `http://localhost:4566` | emulador SQS |
| `SQS_INPUT_QUEUE` / `SQS_DLQ` / `SQS_EVENTS_QUEUE` | `wager-transactions.fifo` / `…-dlq.fifo` / `wager-events.fifo` | filas |
| `SQS_CONSUMER_MAX_ATTEMPTS` | 5 | recebimentos até FAILED + DLQ |
| `SQS_VISIBILITY_TIMEOUT_SECONDS` | 60 | precisa ser ≥ 1,5 × pior caso de processamento; **a instância não sobe** se for menor |
| `SQS_WAIT_TIME_SECONDS` / `SQS_BATCH_SIZE` | 10 / 10 | long polling e tamanho do lote |
| `SHUTDOWN_GRACE_MS` | 10000 (20000 no compose) | prazo para concluir mensagens em andamento no SIGTERM |
| `OUTBOX_BATCH_SIZE` / `OUTBOX_INTERVAL_MS` | 100 / 200 | publisher da outbox |
| `PENDING_REFERENCE_MAX_ATTEMPTS` | 10 | tentativas até `REFERENCE_NOT_FOUND` |
| `PENDING_REFERENCE_BASE_DELAY_MS` / `_MAX_DELAY_MS` | 500 / 30000 | backoff do worker |
| `LOG_LEVEL` | `info` | nível do pino |

### Emulador SQS: LocalStack 4.14.0 (padrão) ou MiniStack

O padrão é `localstack/localstack:4.14.0`, a **última imagem community anterior à consolidação** das
edições do LocalStack. As releases 2026.x exigem `LOCALSTACK_AUTH_TOKEN` e terminam com exit 55 sem
licença. A 4.14.0 roda **sem token** e fica **congelada** de propósito: não deve ser atualizada sem rever
essa restrição.

Alternativa sem dependência de licença, aceita pelo enunciado:

```bash
COMPOSE_PROFILES=ministack docker compose up -d
```

Os dois respondem pelo alias de rede `sqs:4566`. As filas são criadas pelo serviço `sqs-init`, que é
código da própria aplicação e é convergente, então nada mais muda entre um e outro.

## Estrutura

```
src/
  domain/          Money, Wallet, WagerTransaction, ledger, política de liquidação, eventos (TS puro)
  application/     use cases, ports, idempotência (payloadHash), reconciliação, consultas
  infrastructure/  MikroORM (schemas, repositórios, migrations, unit of work), SQS, workers, observabilidade
  interfaces/      controllers HTTP, consumer SQS, contratos (zod)
test/              unit · integration · concurrency · recovery · e2e (multi-instância)
scripts/           test:multi, test:load (orquestrador + gerador)
docker/            Dockerfiles, nginx
```

## Solução de problemas

- **Docker Desktop parado:** "failed to connect to the docker API" significa que é preciso abrir o Docker Desktop.
- **Porta ocupada:** troque o mapeamento no `docker-compose.yml` e ajuste `DATABASE_URL` / `SQS_ENDPOINT`.
- **`bun run test` falha com "app replicas are running":** rode `docker compose stop app nginx`.
- **`ECONNRESET` aos 30 s chamando `localhost:8088` no Windows:** é o proxy de portas do Docker Desktop, não
  a aplicação. A requisição não chega ao nginx. Reenvie com a mesma Idempotency-Key. O teste de carga mede
  de dentro da rede do compose por isso
  ([análise](docs/load-test-analysis.md#1-os-erros-de-rede-eram-do-ambiente-não-da-aplicação)).
