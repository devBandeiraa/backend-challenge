# Distributed Wagering Processor

Serviço financeiro que processa transações de apostas (BET, WIN, LOSS, REFUND, ROLLBACK) vindas de
múltiplos provedores via HTTP e SQS. É correto com entregas duplicadas, fora de ordem e concorrentes,
inclusive com várias instâncias rodando ao mesmo tempo.

- Enunciado: [docs/CHALLENGE.md](docs/CHALLENGE.md)
- Decisões, trade-offs e limitações: [ARCHITECTURE.md](ARCHITECTURE.md)

> Documento em construção: setup e comandos são atualizados a cada fase.

## Requisitos

- Bun 1.4.2: `powershell -c "irm bun.sh/install.ps1 | iex"` (Windows) ou `curl -fsSL https://bun.sh/install | bash`
- Docker com Compose v2.20+ (usa `depends_on.required` e profiles)

## Subindo o ambiente

```bash
bun install
bun run infra:up      # Postgres + emulador SQS + migrations + filas (para rodar app/testes no host)
bun run stack:up      # stack completa: 3 réplicas da app atrás do nginx em http://localhost:8088
bun run infra:down    # derruba tudo e apaga volumes
```

| Serviço | Porta no host |
|---|---|
| PostgreSQL | 5442 |
| SQS (LocalStack/MiniStack) | 4566 |
| nginx → app ×3 | 8088 |

### Emulador SQS: LocalStack 4.14.0 (padrão) ou MiniStack

O padrão é `localstack/localstack:4.14.0`, a **última imagem community anterior à consolidação**
das edições do LocalStack. As releases 2026.x exigem `LOCALSTACK_AUTH_TOKEN` e terminam com exit 55
sem licença. A 4.14.0 roda **sem token** e fica **congelada** nessa versão de propósito: não deve
ser atualizada sem rever essa restrição.

Alternativa sem dependência de licença, aceita pelo enunciado:

```bash
COMPOSE_PROFILES=ministack docker compose up -d
```

Os dois emuladores respondem pelo alias de rede `sqs` na porta 4566. As filas são criadas pelo serviço
one-shot `sqs-init` (código da própria aplicação, idempotente), então nada mais muda entre um e outro.

## Testes

```bash
bun test                  # tudo (precisa de infra:up)
bun run test:unit         # domínio, sem infraestrutura
bun run test:integration  # Postgres e SQS reais
```
