# Arquitetura

O desenho detalhado (modelo, invariantes, estados, recuperação) está em
[`docs/DESIGN.md`](docs/DESIGN.md). Aqui fica o resumo de como o código ficou,
o que eu interpretei do enunciado e o que não deu tempo.

## Visão geral

```
provedor ──HTTP──┐                      ┌── wallet-events.fifo
                 ├─> Wagers.Process ──> Postgres (1 tx SQL) ──> outbox relay ─┘
SQS input ───────┘        ▲
wager-transactions.fifo   └── worker de pendências (PENDING_REFERENCE)
```

HTTP e SQS convergem no mesmo caso de uso (`service.Wagers.Process`) com o
mesmo `domain.Request` e o mesmo hash de payload. Uma operação é uma transação
SQL: trava a carteira, decide, grava transação + lançamento + saldo + eventos
(e a linha da inbox, se veio da fila) e faz commit. Não existe estado
intermediário persistido que precise de compensação.

## Pacotes

| Pacote | Responsabilidade |
|---|---|
| `internal/money` | valor em centavos (`int64`) + moeda, parse estrito, aritmética com overflow |
| `internal/domain` | carteira, transação e máquina de estados, regras de referência, eventos |
| `internal/store` | SQL com pgx; locks, claims com `SKIP LOCKED`, tradução de erros |
| `internal/service` | casos de uso: wagers, pendências, carteiras, inbox, relay da outbox |
| `internal/queue` | adapter SQS: consumidor, DLQ, publisher |
| `internal/httpapi` | rotas, OIDC (Keycloak), mapeamento de erros |
| `internal/app` | wiring com Fx, workers, ordem de shutdown |

Ficou de propósito sem interfaces de repositório: o service fala direto com o
`store`. Com uma base só e testes de integração contra Postgres real, a
abstração não pagava o custo em 3 dias.

## Garantias e onde elas moram

- **Sem saldo negativo e sem débito duplo concorrente**: `SELECT ... FOR UPDATE`
  na carteira + `UPDATE ... WHERE version = $prev`; `CHECK (balance_units >= 0)`
  como última linha de defesa.
- **Idempotência**: índices únicos `(provider, idempotency_key)` e
  `(provider, external_id)`; a checagem é refeita sob o lock; violação de
  unicidade vira lookup + replay. Replay devolve o saldo da época.
- **Ledger imutável**: trigger bloqueia UPDATE/DELETE/TRUNCATE; `CHECK` da
  aritmética `before ± amount = after`; FK composta impede lançamento em outra
  moeda/jogador.
- **Uma reversão por débito/crédito**: índice único parcial em `reference_id`
  para REFUND/ROLLBACK processados.
- **Exactly-once efetivo na fila**: inbox gravada na mesma transação; reentrega
  depois do commit é reconhecida e apagada.
- **Evento nunca se perde**: outbox na mesma transação; relay com lease,
  backoff e sem limite de tentativas. Pode duplicar (crash entre publicar e
  marcar), mas sempre com o mesmo `eventId`.

## Persistência e dinheiro

- **Driver**: pgx v5 (`pgxpool`), SQL escrito à mão, sem ORM. Migrations com
  golang-migrate.
- **Money**: `int64` em centavos + moeda ISO 4217 (`char(3)`, `^[A-Z]{3}$`).
  No banco é `bigint` (`*_units`) + `currency`, nunca `numeric`/float. Limite
  de ±92 quatrilhões com 2 casas; soma/subtração que estoura retorna
  `ErrOverflow`. Na fronteira só aceita a forma canônica
  `(0|[1-9][0-9]*).[0-9]{2}` como string; `1.5`, `1e3`, `+1.00`, `01.00`,
  negativo e número JSON são 400, nunca arredondados.
- **Transação entre repositórios**: `store.InTx(ctx, func(*store.Tx) error)`
  abre uma transação pgx; `store.Tx` embute as mesmas `Queries` do pool, então
  carteira, transação, ledger, outbox e inbox são escritos pelo mesmo
  `pgx.Tx`. Erro ou panic dentro da função faz rollback. A reconciliação usa
  `InSnapshot` (repeatable read, read only) para ler saldo e ledger no mesmo
  instante.

## Idempotência e hash

`domain.Request.Hash()` é SHA-256 (hex) do JSON de um mapa com
`providerId`, `externalTransactionId`, `playerId`, `walletId`, `roundId`,
`gameId`, `kind`, `money{amount,currency}` e, se existir,
`referenceExternalTransactionId`. O `encoding/json` ordena as chaves, o que dá
a forma canônica. Ficam fora a `Idempotency-Key` e metadados de transporte
(`messageId`, `occurredAt`), para HTTP e SQS gerarem o mesmo hash. Não há
normalização além disso: UUIDs saem em minúsculas pelo parse, e o valor só é
aceito já canônico.

Mesma chave + mesmo hash = replay (resultado original, inclusive o saldo da
época). Mesma chave + hash diferente = `IDEMPOTENCY_CONFLICT`. Chave nova com
`externalTransactionId` já usado = `EXTERNAL_ID_REUSED`.

## Contrato HTTP

| Situação | Status | Corpo |
|---|---:|---|
| criada e processada | 201 | transação |
| replay | 200 | transação original |
| pendente de referência | 202 | transação `PENDING_REFERENCE` |
| rejeitada por regra | 422 | transação `REJECTED` com `failureCode` |
| JSON/valor/campo inválido, sem `Idempotency-Key` | 400 | `VALIDATION_ERROR` |
| sem token, token inválido/expirado | 401 | `UNAUTHORIZED` |
| papel errado, `providerId` ≠ claim | 403 | `FORBIDDEN` |
| carteira/transação inexistente | 404 | `WALLET_NOT_FOUND` / `NOT_FOUND` |
| chave ou externalId reutilizados | 409 | `IDEMPOTENCY_CONFLICT` / `EXTERNAL_ID_REUSED` |
| carteira já existe para jogador+moeda | 409 | `WALLET_ALREADY_EXISTS` |
| carteira não é do jogador/moeda informados | 422 | `WALLET_MISMATCH` |
| banco indisponível, timeout | 503 | `SERVICE_UNAVAILABLE` (pode repetir com a mesma chave) |

Rejeição de negócio é 422 *com* a transação gravada: repetir a chamada devolve
a mesma rejeição (é resultado definitivo, não erro).

## Estados e falhas

```
             ┌──> PROCESSED
(entrada) ───┼──> REJECTED
             └──> PENDING_REFERENCE ──> PROCESSED | REJECTED
```

`PROCESSED`, `REJECTED` e `FAILED` são finais (trigger impede mudar). Falha
transitória (banco fora, timeout, conflito de versão sem resultado
concorrente para devolver) não vira estado: rollback, 503 no HTTP ou retry
com backoff na fila. Falha permanente vira `REJECTED`
com código:

| failureCode | Quando | Corrigível pelo provedor? |
|---|---|---|
| `INSUFFICIENT_FUNDS` | BET maior que o saldo | sim, com outra operação (nova chave) |
| `REVERSAL_INSUFFICIENT_FUNDS` | ROLLBACK de WIN/REFUND sem saldo para debitar | não, exige ação manual |
| `REFERENCE_NOT_FOUND` | referência não apareceu até o TTL | sim, enviando a referência e uma nova reversão |
| `REFERENCE_TIMEOUT` | referência existe, mas continuou `PENDING_REFERENCE` até o TTL | sim, quando a referência resolver, com nova reversão |
| `REFERENCE_NOT_PROCESSED` | referência está `REJECTED` | não |
| `REFERENCE_MISMATCH` | provedor, jogador, carteira, moeda ou round diferentes | não |
| `REFERENCE_KIND_NOT_ALLOWED` | combinação de tipos inválida (abaixo) | não |
| `REFERENCE_AMOUNT_MISMATCH` | reversão com valor diferente do original | não |
| `ALREADY_REVERSED` | a referência já tem reversão processada | não |

`INFRASTRUCTURE_FAILURE` (único código de `FAILED`) existe no domínio, mas não
é gravado hoje.

## Combinações de referência

| Operação | Pode referenciar | Efeito |
|---|---|---|
| WIN | BET (opcional) | crédito |
| REFUND | BET | crédito do valor integral |
| ROLLBACK | BET | crédito (desfaz a aposta) |
| ROLLBACK | WIN ou REFUND | débito (desfaz o crédito) |

Reversão é sempre integral e no máximo uma processada por referência (índice
único parcial). ROLLBACK de um REFUND referencia o REFUND, não o BET. Se a
referência ainda está `PENDING_REFERENCE`, a reversão também fica pendente.

## Interpretações do enunciado

- **`PENDING`/`FAILED`**: existem no modelo e na constraint, mas o serviço nunca
  grava `PENDING` (a operação nasce terminal ou `PENDING_REFERENCE`) nem
  `FAILED` (erro de infraestrutura faz rollback e o cliente/fila tenta de novo).
  Mantive os valores para não fechar a porta a um fluxo assíncrono futuro.
- **REFUND/ROLLBACK antes da referência**: vira `PENDING_REFERENCE` (HTTP 202)
  e um worker tenta de novo com backoff até o TTL; ao expirar, `REJECTED` com
  `REFERENCE_NOT_FOUND` (nunca chegou) ou `REFERENCE_TIMEOUT` (chegou, mas
  ficou pendente).
- **ROLLBACK de WIN sem saldo**: `REJECTED` com `REVERSAL_INSUFFICIENT_FUNDS`,
  código separado do `INSUFFICIENT_FUNDS` de aposta para o provedor saber que
  é uma reversão que não pôde ser aplicada.
- **LOSS**: valor zero, sem lançamento e sem `WalletBalanceChanged`; só o
  evento de transação processada.
- **Saldo inicial**: entra como transação `OPENING` + crédito no ledger, para a
  reconciliação fechar desde o primeiro centavo. Saldo inicial zero não gera
  transação.
- **Provedor x carteira**: o `providerId` vem da claim do token; o do corpo só
  é conferido. Um provedor não enxerga transações de outro (404 por id, 403 por
  path).

## Mensageria (SQS FIFO)

**Entrada** `wager-transactions.fifo`: envelope
`{messageId, type: "WagerTransactionRequested", occurredAt, data}`, com `data`
igual ao corpo HTTP + `idempotencyKey`. O produtor usa
`MessageGroupId = walletId` (ordem por carteira, paralelismo entre carteiras)
e `MessageDeduplicationId = messageId`. Fila sem deduplicação por conteúdo,
visibility 30s, redrive para `wager-transactions-dlq.fifo` com
`maxReceiveCount = 5`.

O consumidor recebe até 10 mensagens por long poll e classifica cada uma:

- **ack** (apaga): processada, ou reentrega já presente na inbox;
- **retry** (transitório: banco, timeout): fica na fila e a visibility sobe para
  `2^n` s (teto 5 min); depois de 5 recebimentos o redrive do SQS manda para a DLQ;
- **DLQ imediata** (permanente): JSON inválido, tipo desconhecido, operação
  inválida, carteira inexistente ou de outro jogador, conflito de idempotência,
  `messageId` reutilizado com outro conteúdo. A cópia na DLQ leva os atributos
  `deadLetterReason` e `deadLetterDetail`.

Se uma mensagem de um grupo falha, as seguintes do mesmo grupo no lote voltam
com visibility 0 sem processar, para não quebrar a ordem. Rejeição de negócio
(`INSUFFICIENT_FUNDS` etc.) é ack: o resultado foi gravado e virou evento.

**Saída** `wallet-events.fifo`: o relay da outbox publica com
`MessageGroupId = aggregateId` e `MessageDeduplicationId = eventId`, mais o
atributo `eventType` para roteamento/filtro.

| eventType | aggregateId | Quando |
|---|---|---|
| `WagerTransactionProcessed` | id da transação | PROCESSED |
| `WagerTransactionRejected` | id da transação | REJECTED (com `failureCode`) |
| `WagerTransactionPendingReference` | id da transação | entrou em PENDING_REFERENCE |
| `WalletBalanceChanged` | id da carteira | houve lançamento no ledger |

Envelope: `{eventId, eventType, aggregateId, correlationId, causationId,
occurredAt, version, data}`; `correlationId` é a `Idempotency-Key` e
`causationId` é o `X-Request-Id` ou o `messageId` de origem. Consumidores devem
deduplicar por `eventId`: a entrega é at-least-once (a deduplicação do SQS só
cobre 5 minutos).

## Identidade e autorização

**IdP: Keycloak** (realm `wager` importado de `deploy/keycloak`). Escolhido por
rodar local em container, falar OIDC padrão (client credentials, JWKS) e
permitir colocar `provider_id` como claim fixa por client, que é exatamente o
que o serviço precisa. Em produção poderia ser Cognito/Auth0 sem mudar o
código, só as variáveis `OIDC_*`.

Validação em cada request (`httpapi.Verifier`, go-oidc): assinatura RS256 com
chave do JWKS (cacheada, renovada quando aparece `kid` novo), `iss` igual a
`OIDC_ISSUER`, `aud` contendo `wager-wallet-api`, `exp` no futuro e
`typ = Bearer` (recusa ID token/refresh). Token expirado é 401.

| Papel (realm role) | Clients | Pode |
|---|---|---|
| `provider` + claim `provider_id` | `provider-a`, `provider-b` | enviar operações e ler só as próprias |
| `wallet-operator` | `wallet-internal` | abrir/consultar carteiras, ledger, reconciliação, ler qualquer transação |

O `providerId` nunca vem da requisição: é da claim. Corpo ou path com outro
provedor é 403; transação de outro provedor buscada por id é 404 (não revela
que existe).

**Broker**: no LocalStack as credenciais são `test/test` e não há política
aplicada. Em AWS cada instância roda com uma role de menor privilégio:

```json
{
  "Statement": [
    { "Effect": "Allow",
      "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes"],
      "Resource": "arn:aws:sqs:*:*:wager-transactions.fifo" },
    { "Effect": "Allow", "Action": "sqs:SendMessage",
      "Resource": ["arn:aws:sqs:*:*:wager-transactions-dlq.fifo", "arn:aws:sqs:*:*:wallet-events.fifo"] }
  ]
}
```

Provedores recebem só `sqs:SendMessage` na fila de entrada; consumidores de
eventos, só leitura em `wallet-events.fifo`.

## Operação

- Readiness checa Postgres e a fila de entrada; liveness não depende de nada.
- Shutdown: HTTP para de aceitar, workers param de buscar trabalho e terminam
  o que já pegaram (mensagens recebidas e não iniciadas voltam com visibilidade
  0), pool fecha por último.
- Logs JSON com `instance`, `walletId`, `transactionId`, `messageId` quando
  existem. Métricas Prometheus em `/metrics` (operações, conflitos, retries,
  DLQ, backlog e atraso da outbox).

## Não deu tempo

- **IAM aplicado**: a política acima está só documentada; nada no ambiente
  local a exercita.
- **Redrive da DLQ**: as mensagens chegam lá com motivo nos atributos, mas não
  há ferramenta para reprocessar.
- **Reconciliação agendada**: hoje é sob demanda por carteira. Faltou o job
  que varre tudo e alerta.
- **Limpeza de inbox e outbox publicada**: as tabelas só crescem; precisa de
  retenção (ex.: 7 dias) ou particionamento.
- **Rate limit por provedor** e limites de valor por operação.
- **Tracing** (OpenTelemetry). Hoje a correlação é por `X-Request-Id` e
  `correlationId` nos eventos.
- **Multi-moeda por jogador** está no modelo (`UNIQUE(player_id, currency)`),
  mas só testei BRL.
- **Testes de carga**: os testes de concorrência provam correção, não
  throughput.
