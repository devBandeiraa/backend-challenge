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
transações órfãs. O HTTP responde 422 `WALLET_NOT_FOUND`. No SQS é erro de negócio, com **ack e sem inbox**:
a exceção desfaz a transação inteira, mas não há efeito financeiro a proteger. Uma redelivery só
recalcula o mesmo resultado (e, se a wallet tiver sido criada nesse meio-tempo, processa corretamente).

### Quando `FAILED` é gravado
O HTTP **nunca** grava FAILED: falha transitória responde 503 e o cliente reenvia com a mesma key.
FAILED (`PROCESSING_FAILED`) é gravado numa **transação separada, curta e best-effort**:

- pelo consumer SQS, quando esgota os retries transitórios (`ApproximateReceiveCount ≥ MAX`)
  ou encontra um erro inesperado não classificado. Depois de gravar, envia a mensagem
  explicitamente para a DLQ e a remove da fila de origem;
- pelo worker de PENDING_REFERENCE, quando a resolução de uma linha lança erro **não transitório**
  (bug, dado inesperado). Sem isso, uma linha "venenosa" seria reivindicada em loop para sempre.
  Erros transitórios só deixam a linha pendente para o próximo ciclo.

Detalhes:
- A gravação do FAILED **não trava a wallet**: o INSERT pega só o `FOR KEY SHARE` da FK.
  Por isso ela funciona mesmo quando a causa dos retries é justamente a wallet disputada (há teste disso).
- Se a transação já existir (por exemplo, PENDING_REFERENCE ou terminal), ela não é sobrescrita.
- FAILED não emite evento de integração, porque não está na lista do enunciado. O sinal operacional é a DLQ
  e a métrica `wagering_dlq_messages_total{reason}`.

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

## Decisões da Fase 1 (domínio, aplicação, HTTP)

### Money
- `decimal.js` num construtor **isolado** (`Decimal.clone`, precisão 40), que não altera a configuração global da lib.
  Com 19 dígitos de NUMERIC(19,2), `add`/`subtract` são sempre exatos.
- **Escala fixa no contrato**, como diz o README: `amount` tem **exatamente 2 casas** (`"25.00"`).
  `"25"` e `"25.5"` são rejeitados com 400. Mais de 2 casas também é **rejeitado, nunca arredondado**,
  porque arredondar dinheiro em silêncio esconde erro do provedor.
- **Também rejeitados:** sinal, notação científica, `NaN`/`Infinity`, vazio, zero à esquerda, espaços,
  vírgula, valor que não seja string e qualquer valor acima da faixa de NUMERIC(19,2).
- Negativos só surgem por aritmética interna (`negate`/`subtract`), nunca pela entrada.
- O banco devolve `numeric` como **string**, mapeado com `p.decimal('string')`. Em nenhum ponto
  entre o Postgres e o `Money` o valor passa por `number`.
- Como cada valor tem uma única grafia válida no contrato, o payloadHash não depende de normalização.

### payloadHash
`hex(SHA-256(UTF-8(canonicalJson({ v: "wager-payload/v1", ...campos de negócio }))))`
- **Campos de negócio:** providerId, externalTransactionId, playerId, walletId, roundId, gameId, kind,
  money{amount,currency}, referenceExternalTransactionId.
- **Fora do hash:** o header Idempotency-Key, messageId, occurredAt e correlationId.
- **JSON canônico:** chaves ordenadas recursivamente, propriedades `undefined` omitidas e `number`
  proibido (serialização de float não é canônica entre runtimes).
- A versão do algoritmo entra no conteúdo hasheado, o que permite evoluí-lo sem colidir com hashes antigos.

### Regras de negócio: interpretações adotadas
- **Ordem de verificação (`SettlementPolicy`):**
  1. player e moeda da wallet;
  2. contexto da referência;
  3. kind da referência;
  4. status da referência;
  5. valor e reversão anterior;
  6. saldo.

  O que invalida a operação para sempre é verificado antes do que depende de estado.
- **WIN pode referenciar a BET da rodada** (o README diz "pode"). Quando referencia, valem as mesmas
  checagens de contexto e kind (só BET), mas **sem** igualdade de valor (um WIN pode pagar qualquer valor).
  Se a referência não chegou, a transação fica em PENDING_REFERENCE.
- **⚠️ Extensão: LOSS com referência.** O README não prevê referência em LOSS. Aceito opcionalmente,
  com as mesmas regras do WIN, porque um provedor que liga o resultado à aposta deve receber a mesma
  validação de contexto (player/wallet/moeda/rodada e kind BET) em vez de ter a referência ignorada.
  Sem referência, LOSS funciona exatamente como o README descreve.
- **LOSS** aceita valor `0.00` ou positivo, não gera lançamento, não altera `version` e gera só `WagerTransactionProcessed`.
- **⚠️ WIN rejeitado por referência ausente.** Um WIN **com** `referenceExternalTransactionId` cuja BET
  nunca chega fica em PENDING_REFERENCE durante a janela do worker (10 tentativas, cerca de 3 min). Esgotada
  a janela, vira `REJECTED`/`REFERENCE_NOT_FOUND`, **sem crédito**, e o evento `WagerTransactionRejected`
  é publicado.
  - **Consequência:** a referência declarada é tratada como pré-condição. Não creditar um prêmio cuja
    aposta não existe é o comportamento conservador: nunca credita dinheiro que não deveria.
  - **Contrapartida:** um prêmio legítimo com referência errada fica retido. O provedor precisa reenviar
    o WIN com novo `externalTransactionId`, seja sem referência, seja com a referência correta.
  - **Mesma regra para LOSS:** se a referência não chegar, é rejeitado igualmente, sem efeito financeiro.
- **Referência de outra wallet, player ou rodada** é rejeitada com `REFERENCE_MISMATCH` **sem gravar**
  `reference_transaction_id`, porque a FK composta só aceita referência da mesma wallet.
- **Referência existente, mas PENDING/PENDING_REFERENCE** (ex.: ROLLBACK de um REFUND que ainda espera a BET):
  a transação também aguarda.
- **Referência REJECTED/FAILED:** rejeitada com `REFERENCE_NOT_PROCESSED`.
- **Snapshot do resultado** (`result_balance`, `result_wallet_version`) é gravado em toda transação PROCESSED
  e REJECTED. É o que o replay devolve ("o saldo observado naquele momento"), inclusive para LOSS e
  REJECTED, que não têm ledger. Na rejeição por moeda, o saldo vem na moeda da wallet.

### Taxonomia de failureCode
| Código | Ação do provedor | Quando |
|---|---|---|
| `INSUFFICIENT_FUNDS` | retry_later (nova transação) | BET maior que o saldo |
| `REVERSAL_WOULD_OVERDRAW` | give_up (operacional) | ROLLBACK que debitaria (de WIN/REFUND) deixaria saldo negativo |
| `REFERENCE_NOT_FOUND` | fix_payload | referência não chegou dentro da janela de retry |
| `REFERENCE_MISMATCH` | fix_payload | referência de outro player/wallet/moeda/rodada |
| `REFERENCE_ALREADY_REVERSED` | give_up | já existe reversão efetiva (decisão B) |
| `REFERENCE_NOT_PROCESSED` | give_up | referência REJECTED/FAILED |
| `AMOUNT_MISMATCH` | fix_payload | valor da reversão difere do valor da referência |
| `CURRENCY_MISMATCH` | fix_payload | moeda da operação difere da wallet |
| `INVALID_REFERENCE_KIND` | fix_payload | REFUND→BET; ROLLBACK→BET/WIN/REFUND; WIN/LOSS→BET |
| `PLAYER_MISMATCH` | fix_payload | wallet não pertence ao player |
| `WALLET_NOT_FOUND` | fix_payload | walletId inexistente (não persistido) |
| `PROCESSING_FAILED` | retry_later (nova transação) | status FAILED |

Toda rejeição é terminal **para aquela Idempotency-Key**: reenviar com a mesma key devolve o mesmo
resultado. "retry_later" significa enviar uma nova transação. O catálogo está em
[failure-code.ts](src/domain/wagering/failure-code.ts).

### HTTP: um mapeamento para todos os endpoints
| Status | Significado | O provedor deve |
|---|---|---|
| 200 | PROCESSED (primeira vez **ou** replay; `idempotentReplay` distingue) | seguir |
| 201 | wallet criada | seguir |
| 202 | PENDING_REFERENCE | aguardar; consultar o GET ou o evento |
| 400 | payload/header/parâmetro inválido (zod `strict` + validação do Money) | corrigir |
| 404 | recurso de consulta inexistente | — |
| 409 | `IDEMPOTENCY_KEY_PAYLOAD_MISMATCH`, `EXTERNAL_TRANSACTION_ID_CONFLICT`, `WALLET_ALREADY_EXISTS` | não reenviar igual |
| 422 | REJECTED/FAILED com `failureCode`; mesmo corpo no replay | ver taxonomia |
| 503 | falha transitória (`Retry-After`) | reenviar com a **mesma** key |

- **Status do replay:** sai do **resultado**, então o replay recebe o mesmo status **e** o mesmo corpo
  da primeira resposta, exceto `idempotentReplay`. Há teste para 200, 422 e 202 e para `WALLET_NOT_FOUND`.
  Por isso nenhum corpo de resultado carrega campo volátil: o correlationId vai só no header `X-Correlation-Id`.
- **Erros:** um único `HttpErrorFilter` faz o mapeamento categoria→status. Controllers não escolhem
  status de erro.
- **Campos desconhecidos:** o zod `strict` responde 400. Num WIN, um typo em
  `referenceExternalTransactionId` seria descartado em silêncio.

### Persistência e transação
- **Isolamento:** READ COMMITTED + `FOR NO KEY UPDATE` na wallet. A serialização vem do lock por wallet,
  não do nível de isolamento. Com SERIALIZABLE, todo conflito em hot wallet viraria abort (`40001`) e
  retry caro, em vez de uma espera curta.
- **Escritas:** nativas (`em.insert`/`nativeUpdate`) em vez de `flush` do Unit of Work, porque a ordem
  dos comandos importa (FKs e o trigger de encadeamento do ledger) e precisa ficar explícita.
  O `em.transactional` continua definindo a fronteira da transação.
- **Leituras:** `disableIdentityMap`, para sempre ver o estado confirmado/travado no banco.
- **Retry da unit of work:** repete a transação inteira para `40001`, `40P01`, `55P03` e para a violação de
  `wt_single_reversal_uq`. São até 5 tentativas (`UOW_MAX_ATTEMPTS`), com backoff de 10 a 200 ms e jitter,
  cada uma num `fork` novo do EntityManager.
  - **Métrica:** **cada retry** incrementa `wagering_db_lock_conflicts_total{sqlstate}`.
  - **Esgotadas as tentativas:** incrementa `wagering_db_retries_exhausted_total`, e o HTTP responde
    **503 com `Retry-After: 1`**. Com o banco inacessível, a resposta também é 503.
- **`lock_timeout` por transação:** `SET LOCAL lock_timeout`, 5 s por padrão (`DB_LOCK_TIMEOUT_MS`).
  Sem teto, uma hot wallet sob carga extrema empilharia conexões esperando indefinidamente. Com ele,
  a espera vira `55P03`, que segue para retry com backoff e, se persistir, para 503: uma degradação
  explícita e mensurável. O teste [transient-failures.test.ts](test/integration/transient-failures.test.ts)
  segura um lock real sobre a wallet e verifica o 503, o `Retry-After`, um incremento da métrica por retry
  e, depois do release, o reenvio com a mesma key processando normalmente.
- **Classificação de erros:** por SQLSTATE e nome de constraint, nunca por mensagem. Os triggers usam
  SQLSTATEs próprios (`WL001`–`WL004`, `WT001`).
- **Wallet inexistente:** verificada **sem lock** antes do insert-first. Como wallets nunca são apagadas
  (trigger), o resultado não fica obsoleto, e evita que a FK aborte a transação.
- **Ids:** UUIDv7 (`Bun.randomUUIDv7`), ordenáveis por tempo (localidade de índice) e sem coordenação entre instâncias.

### Ledger: paginação
- **Cursor opaco:** `base64url({ w: walletId, v: wallet_version })`, em ordem decrescente.
- **Estabilidade:** `wallet_version` é imutável e único por wallet, então novos lançamentos não deslocam
  as páginas, diferente de OFFSET. Isso tem teste.
- **Reuso indevido:** um cursor de outra wallet resulta em 400.

### Fronteiras (hexagonal)
- **`domain/`:** sem Nest e sem ORM. Recebe ids e datas de fora (`IdGenerator`, `Clock`), o que o mantém determinístico.
- **`application/`:** casos de uso como classes puras. O Nest só as monta por factory (`app.module.ts`).
- **`infrastructure/`:** os registros de persistência (`defineEntity`) são separados dos agregados.
  A reidratação passa sempre por `rehydrate`.

## Decisões da Fase 2 (mensageria, workers, concorrência)

### Consumer SQS
- **Mesmo use case do HTTP:** `executeFromMessage` envolve o mesmo `process()` e adiciona a inbox
  **na mesma transação SQL** de transação, wallet, ledger e outbox.
- **Duas camadas de deduplicação:**
  - **inbox**, por `(consumer_name, message_id)`, de transporte: o SQS reentregou a mesma mensagem;
  - **idempotency key**, de negócio: o provedor reenviou a mesma operação com outro messageId.
  - Numa redelivery, o fluxo segue o caminho normal, e o insert-first devolve o resultado original
    sem efeito. O ack sempre corresponde a um resultado real.
- **Envelope tolerante, `data` estrito:**
  - No envelope, só `data` é obrigatório. `messageId`, `type` e `occurredAt` são opcionais, e campos
    extras passam (uma mudança de envelope do produtor não pode mandar dinheiro para a DLQ).
  - Sem `messageId`, usa o `MessageId` do SQS, que é estável entre redeliveries.
  - O `data` é zod `strict`, com `idempotencyKey` obrigatória, e o conteúdo do Money é validado pelo domínio.
  - O hash da inbox é o SHA-256 do `data` canônico. O mesmo messageId com conteúdo diferente resulta
    em `INBOX_PAYLOAD_MISMATCH`, que vai para a DLQ.
- **Ack só depois do commit.** Um crash entre o commit e o ack causa redelivery, que a inbox reconhece:
  nenhum efeito novo. Isso é provado com **processo real** encerrado com `exit 137` no hook pós-commit
  ([crash-after-commit.test.ts](test/concurrency/crash-after-commit.test.ts)).
- **Classificação de erros:**

| Erro | Ação | FAILED? |
|---|---|---|
| REJECTED (negócio), `WALLET_NOT_FOUND` | ack | — |
| JSON/envelope/`data` inválido, `INVALID_MONEY`, conflito de idempotência, `INBOX_PAYLOAD_MISMATCH` | DLQ (motivo em atributo) | não: não há transação válida e única |
| transitório, com `receiveCount < SQS_CONSUMER_MAX_ATTEMPTS` (5) | `ChangeMessageVisibility` com backoff exponencial (1 s → 60 s) | — |
| transitório no limite, ou erro inesperado | grava FAILED (transação separada) e depois DLQ | sim |

- **Limite do consumer vs. redrive:** o consumer desiste no 5º recebimento, e o redrive do SQS
  (`maxReceiveCount=5`) só moveria no 6º. O redrive é a rede de segurança para crash loop, em que
  o consumer morre antes de decidir.
- **Mensagem na DLQ:** é enviada com o corpo original, o mesmo `MessageGroupId`, `MessageDeduplicationId`
  igual ao id SQS original e os atributos `failureReason`, `receiveCount` e `originalMessageId`.
  Só depois a mensagem é apagada da origem.
- **Concorrência:** sequencial **dentro** de um `MessageGroupId` (walletId), paralela **entre** grupos.
  - **Trade-off:** uma mensagem em retry segura a fila daquela wallet (comportamento FIFO) até ser
    processada ou ir para a DLQ. A ordem por wallet é preservada ao custo de latência só para aquela wallet.
- **Visibility timeout de 30 s vs. pior caso:** são 5 tentativas da unit of work × `lock_timeout` de 5 s,
  mais backoff, o que dá cerca de 26 s, abaixo de 30 s. Se esses parâmetros mudarem, o visibility timeout
  precisa acompanhar.

### SIGTERM / shutdown
1. O Nest (`enableShutdownHooks`) chama `beforeApplicationShutdown` **antes** de o ORM fechar o pool.
2. O consumer para de receber (aborta o long polling) e conclui o que está em andamento
   (`SHUTDOWN_GRACE_MS`, 20 s no compose).
3. Mensagens já recebidas que não começaram voltam **imediatamente** para a fila (`ChangeMessageVisibility 0`).
4. O que exceder o prazo volta sozinho quando o visibility timeout expirar.
5. Workers de polling param cooperativamente: o tick corrente termina, nunca no meio de uma transação.

No compose:
- `init: true` (tini como PID 1): o Nest re-emite o sinal depois do shutdown, e o PID 1 ignoraria sinais
  sem handler.
- `stop_grace_period: 30s`, maior que o prazo de 20 s do consumer.

**Evidência:**
- [sqs-consumer.test.ts](test/integration/sqs-consumer.test.ts): a mensagem em andamento conclui e a
  não iniciada é reprocessada por outro consumer em segundos, não em 30 s.
- [multi-instance.e2e.ts](test/e2e/multi-instance.e2e.ts): `docker kill --signal SIGTERM` numa réplica
  durante o consumo de 120 mensagens. Resultado: logs de shutdown cooperativo, saída 143, nenhuma mensagem
  perdida ou duplicada e a réplica volta healthy depois de `docker start`.
  O Docker **não** aplica restart policy após `docker kill`; em produção, quem recria é o orquestrador.

### Outbox publisher
- **Reivindicação:** `em.find(..., { lockMode: LockMode.PESSIMISTIC_PARTIAL_WRITE })`, que gera
  `FOR UPDATE SKIP LOCKED`, em lotes. Cada publisher pega um lote diferente.
- **Publicação:** `SendMessageBatch` com `MessageGroupId = aggregateId` e `MessageDeduplicationId = eventId`.
  `published_at` é marcado na mesma transação (trade-off já registrado).
- **Falha no SQS:** o lote é reagendado com o backoff da outbox (`OutboxMessage.scheduleRetry`, até
  5 min e nunca descartado). Há teste que apaga a fila de destino de verdade.
- **Ordenação:** dentro de um lote, segue `next_attempt_at, occurred_at`. **Entre publishers concorrentes,
  a ordem por agregado não é garantida.** Os consumidores devem usar `walletVersion` (em
  `WalletBalanceChanged`) para ordenar e o `eventId` para deduplicar. Garantir ordem global exigiria um
  publisher único por agregado (particionamento), o que não compensa aqui.
- **Métricas:** `wagering_outbox_lag_seconds` (idade do pendente mais antigo, pelo relógio do banco) e
  `wagering_outbox_pending_messages`, atualizadas no máximo uma vez por segundo por instância.

### Worker de PENDING_REFERENCE
- **Um item por transação:** `claimDuePendingReference` (`FOR UPDATE SKIP LOCKED`), depois o lock da wallet
  e o **mesmo** `TransactionSettler` da submissão.
- **Tentativas:** `attempts` conta as tentativas de resolução, incluindo o processamento inicial. Na tentativa
  `PENDING_REFERENCE_MAX_ATTEMPTS` (10), a transação vira `REJECTED`/`REFERENCE_NOT_FOUND` com
  `WagerTransactionRejected`.
- **Evento:** `WagerTransactionPendingReference` é emitido **uma vez**, na entrada; reagendamentos não repetem.
- **Cadeias fora de ordem** (`ROLLBACK(REFUND)` → `REFUND` → `BET`) se resolvem em ordem causal, porque
  uma referência ainda pendente mantém o dependente aguardando. Há teste disso.

### Métricas (por instância, `GET /metrics`)
| Métrica | O que mede |
|---|---|
| `wagering_transactions_total{status,source,replay}` | transações por status e origem (http/sqs) |
| `wagering_duplicates_detected_total{layer}` | duplicatas: `idempotency` ou `inbox` |
| `wagering_db_lock_conflicts_total{sqlstate}` | **cada** retry por conflito (deadlock, lock timeout, serialization) |
| `wagering_db_retries_exhausted_total` | unit of work que desistiu (vira 503 ou retry SQS) |
| `wagering_sqs_messages_total{outcome}`, `wagering_sqs_retries_total` | desfecho de cada mensagem; retries com backoff |
| `wagering_dlq_messages_total{reason}` | mensagens movidas para a DLQ |
| `wagering_outbox_published_total`, `..._publish_failures_total`, `..._lag_seconds`, `..._pending_messages` | outbox |
| `wagering_pending_reference_resolutions_total{result}` | resultado das tentativas do worker |
| `wagering_processing_duration_seconds{source}` | latência (histograma) |

### Estratégia de testes
- **Infraestrutura real em tudo:** Postgres e LocalStack em todos os testes de integração e concorrência.
  O único "spy" é um wrapper que **delega** ao gateway SQS real, para contar envios por eventId. A única
  injeção de falha de programação (linha venenosa do worker) também roda sobre o banco real.
- **Filas exclusivas por teste** (`createTestQueues`): nenhuma mensagem vaza entre testes.
- **Concorrência:**
  - barreira (`inParallel`): todas as operações são criadas e só disparam quando todas estão prontas;
  - 3 contextos de aplicação com **pools próprios**, como 3 instâncias;
  - cenários repetidos (100/80/80 em 20 rodadas, reversões concorrentes em 10) para exercitar várias intercalações.
- **`assertLedgerInvariant()`** ao final de cada teste de concorrência e de cada wallet tocada: saldo
  armazenado igual à soma do ledger, e `version` igual à versão do último lançamento, calculados no banco
  com NUMERIC exato.
- **3 instâncias de verdade:** `bun run test:multi` sobe o compose (3 réplicas com pool, consumer,
  publisher e worker próprios, atrás do nginx), verifica pelo header `X-Instance-Id` que ≥ 3 réplicas
  atenderam, roda os cenários pelo nginx e desliga as réplicas no fim.
- **Guard:** `bun run test` recusa rodar com réplicas no ar, porque publishers e workers delas
  concorreriam com as medições.
- **Timeout:** passado por `--timeout` nos scripts. Um `setDefaultTimeout` no preload só valeria para o
  primeiro arquivo da execução.
- **Instabilidade observada (honesta):** na primeira execução do multi-instância, logo após um rebuild
  do stack, uma requisição da rajada de 50 recebeu "socket closed" no cliente sem registro no nginx.
  Não reproduziu em 4 execuções seguintes. A causa provável, **não confirmada**, é o reuso de uma conexão
  keep-alive do cliente durante a recriação dos containers.
  O comportamento correto de um provedor nesse caso é reenviar com a mesma Idempotency-Key, o que é seguro.

## Stack e ambiente

| Item | Versão | Observação |
|---|---|---|
| Bun | 1.4.2 | runtime, package manager, test runner (mesma versão no host e na imagem) |
| NestJS | 12.1.2 | ESM; DI por `emitDecoratorMetadata` funciona no Bun |
| MikroORM | 7.2.3 | driver `pg`; migrations por `migrationsList` explícita |
| PostgreSQL | 17.11 | |
| LocalStack | 4.14.0 | ver abaixo |

Dependências fixadas em versões publicadas há pelo menos 2 semanas.

### Emulador SQS: LocalStack 4.14.0 congelado, MiniStack como alternativa

- **Contexto:** a partir das releases 2026.x, o LocalStack consolidou as edições numa imagem única
  que exige `LOCALSTACK_AUTH_TOKEN`. Sem licença, o container termina com exit 55
  (verificado com `2026.08.5`).
- **Decisão:** usar `localstack/localstack:4.14.0` (fev/2026), a última imagem community
  **pré-consolidação**. Ela roda **sem token**, e o suporte a SQS FIFO + RedrivePolicy foi verificado.
  A versão fica **congelada**: atualizar para 2026.x quebraria o setup de quem não tem conta LocalStack.
- **Alternativa:** MiniStack (`ministackorg/ministack:1.5.27`, MIT, citado no enunciado) via profile:
  `COMPOSE_PROFILES=ministack docker compose up -d`. Também verificado: as 3 réplicas sobem healthy.
- **Como os dois são intercambiáveis:**
  - os dois serviços expõem o alias de rede `sqs:4566`;
  - as filas são criadas por código da aplicação ([provision-queues.ts](src/infrastructure/messaging/sqs/provision-queues.ts)),
    idempotente e executado pelo serviço one-shot `sqs-init` e pelo setup dos testes, em vez do init script
    `ready.d` do LocalStack, que só existe nele;
  - o profile padrão vem de `.env` (`COMPOSE_PROFILES=localstack`), que contém só defaults não sensíveis.
- **Trade-off:** 4.14.0 não recebe mais correções. Como é só emulador de SQS para dev/teste, o risco é baixo,
  e o MiniStack cobre o caso de incompatibilidade futura.

### Compose: 3 réplicas atrás do nginx
`app` roda com `deploy.replicas: 3`, e o nginx (porta 8088) distribui as requisições usando o DNS do compose.
`proxy_next_upstream off` impede o nginx de reenviar um POST para outra réplica: o retry é decisão do
cliente, que tem a Idempotency-Key. As migrations rodam num serviço one-shot (`migrate`) antes
das réplicas, e não no boot de cada uma, para evitar 3 instâncias disputando o mesmo schema.
