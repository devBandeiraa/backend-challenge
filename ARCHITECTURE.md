# ARCHITECTURE

> Documento vivo: cada decisão entra aqui quando é tomada. A versão final (Fase 3) consolida
> diagramas, fluxos e limitações.

## Decisões registradas

### ⚠️ B — Uma única reversão efetiva por referência (mais restrito que o enunciado)

O enunciado exige que "uma referência não pode ser revertida duas vezes **pelo mesmo tipo de operação**".
Aplicada ao pé da letra, essa regra permite:

```
BET 100        → saldo -100
REFUND(BET)    → saldo +100   (1ª reversão, kind REFUND)
ROLLBACK(BET)  → saldo +100   (2ª reversão, kind ROLLBACK)  ← crédito duplicado
```

Crédito duplicado é falha eliminatória. Por isso o schema impõe **no máximo uma reversão `PROCESSED`
por referência, de qualquer kind**:

```sql
CREATE UNIQUE INDEX wt_single_reversal_uq ON wager_transactions (reference_transaction_id)
  WHERE status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK');
```

- Cobre a regra do enunciado: dois REFUND da mesma BET continuam impossíveis.
- `ROLLBACK(REFUND)` continua permitido porque referencia o REFUND, não a BET.
- A segunda tentativa é rejeitada com `REFERENCE_ALREADY_REVERSED`.

### A — Lock da wallet com `FOR NO KEY UPDATE`

O fluxo é insert-first: o `INSERT` em `wager_transactions` valida a FK `wallet_id` e, para isso,
o Postgres pega `FOR KEY SHARE` na linha da wallet. Como `FOR UPDATE` conflita com `FOR KEY SHARE`,
dois escritores na mesma wallet produzem um ciclo de espera:

1. A insere a transação e B insere a sua, de modo que as duas seguram KEY SHARE na wallet.
2. A pede FOR UPDATE e espera B.
3. B pede FOR UPDATE e espera A. O Postgres detecta o deadlock e aborta uma delas com `40P01`.

`FOR NO KEY UPDATE` serializa os escritores entre si (é o mesmo lock que o próprio `UPDATE balance`
pega, já que nenhuma coluna de chave muda) e não conflita com KEY SHARE.
**Evidência:** `test/integration/wallet-lock-mode.test.ts` reproduz os dois casos no Postgres real.
O `LockMode` do MikroORM só gera `FOR UPDATE`, então esse lock é SQL explícito no repositório.
`LockMode.PESSIMISTIC_PARTIAL_WRITE` (`FOR UPDATE SKIP LOCKED`) é usado no outbox e no worker de referências.

### C — `INSERT ... ON CONFLICT DO NOTHING` no insert-first

No Postgres, qualquer erro dentro de uma transação a aborta, então capturar `unique_violation`
exigiria um savepoint. `ON CONFLICT DO NOTHING` mantém a mesma semântica: uma duplicata concorrente
**espera** no índice único até a original confirmar ou desfazer. Se a original fizer rollback,
a duplicata assume o processamento. Quando nenhuma linha é inserida:

| Situação | Resposta |
|---|---|
| mesma `idempotency_key`, mesmo `payload_hash` | replay do resultado gravado, `idempotentReplay: true` |
| mesma `idempotency_key`, hash diferente | 409 `IDEMPOTENCY_KEY_PAYLOAD_MISMATCH` |
| mesmo `(provider_id, external_transaction_id)`, key diferente | 409 `EXTERNAL_TRANSACTION_ID_CONFLICT` |

### D — `WALLET_NOT_FOUND` não é persistido
`wager_transactions.wallet_id` tem FK para `wallets`: preferi integridade referencial a persistir
transações órfãs. O HTTP responde 422 `WALLET_NOT_FOUND`. No SQS é erro de negócio: grava a inbox (sem `transaction_id`) e faz ack.

### Quando `FAILED` é gravado
O HTTP **nunca** grava FAILED: falha transitória responde 503 e o cliente reenvia com a mesma key.
FAILED (`PROCESSING_FAILED`) é gravado numa **transação separada, curta e best-effort**:

- pelo consumer SQS, quando esgota os retries transitórios (`ApproximateReceiveCount ≥ MAX`)
  ou encontra um erro inesperado não classificado. Depois de gravar, envia a mensagem
  explicitamente para a DLQ e a remove da fila de origem;
- pelo worker de PENDING_REFERENCE, em erro permanente não relacionado à referência.

Se nem a gravação do FAILED for possível (banco fora), a mensagem vai para a DLQ sem o registro,
e o redrive a reprocessa depois. FAILED é terminal para aquela key: o replay devolve 422
`PROCESSING_FAILED`. Payload que não passa no parse vai direto para a DLQ, sem FAILED, porque
não há transação válida para gravar.

### Violação de `wt_single_reversal_uq` é transitória
Sob o lock da wallet, a regra de aplicação já detecta a dupla reversão. O índice é a rede de segurança.
Se ele disparar (`23505` nesse índice), a unit of work inteira é refeita, com número limitado de tentativas,
como acontece com `40001`/`40P01`/`55P03`. Na nova tentativa a regra encontra a reversão confirmada
e rejeita com `REFERENCE_ALREADY_REVERSED`.

### Fila de entrada: `MessageGroupId = walletId`
Ordena as mensagens por wallet e paraleliza entre wallets, alinhado à unidade de concorrência.
É **otimização**: a correção não depende dela (o lock e as constraints do banco garantem os invariantes).

### F — Eventos publicados em `wager-events.fifo`
`MessageGroupId = aggregateId`, `MessageDeduplicationId = eventId`.

### Trade-off: outbox segura a transação durante o publish
O publisher faz `SELECT ... FOR UPDATE SKIP LOCKED`, chama `SendMessage` e marca `published_at`
**dentro da mesma transação**.
- **Custo:** uma conexão do pool e os locks das linhas ficam presos durante I/O de rede.
  Se o SQS ficar lento, o pool sofre.
- **Duplicata:** um crash entre o `SendMessage` e o COMMIT causa republicação. É aceitável porque a entrega
  é at-least-once por contrato e o consumidor deduplica por `eventId` (a FIFO também deduplica
  na janela de 5 min).
- **Alternativa:** lease (`claimed_until`) com duas transações curtas, que solta a conexão durante o I/O
  em troca de mais estados e um intervalo de lease para calibrar.

### G — Outbox nunca descarta
O backoff exponencial tem teto, e o evento nunca é descartado (perder evento confirmado é eliminatório).
O atraso fica visível na métrica de outbox lag. O limite de tentativas vale para PENDING_REFERENCE:
ao esgotar, a transação vira `REJECTED`/`REFERENCE_NOT_FOUND` e o evento correspondente é publicado.

## Stack e ambiente

| Item | Versão | Observação |
|---|---|---|
| Bun | 1.4.3 | runtime, package manager, test runner |
| NestJS | 12.1.2 | ESM; DI por `emitDecoratorMetadata` funciona no Bun |
| MikroORM | 7.2.3 | driver `pg`; migrations por `migrationsList` explícita |
| PostgreSQL | 17.11 | |
| LocalStack | 4.14.0 | ver abaixo |

Dependências fixadas em versões publicadas há pelo menos 2 semanas.

**LocalStack:** as imagens 2026.x terminam com exit 55 sem `LOCALSTACK_AUTH_TOKEN`. A 4.14.0 é a última
community que roda sem licença e suporta SQS FIFO + redrive. Fallback testado: MiniStack
(`ministackorg/ministack:1.5.27`), que o enunciado também aceita.
