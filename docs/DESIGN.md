# Design (antes do código)

Rascunho feito antes de implementar. Serve de mapa: se o código divergir daqui,
o código está errado ou este documento precisa ser atualizado.

## 1. Modelo de dados

```
wallets                 wager_transactions                 wallet_ledger_entries
-------                 ------------------                 ---------------------
id (pk)                 id (pk)                            id (pk)
player_id               origin  INTERNAL | EXTERNAL        seq (identity, ordem estável)
currency                kind    OPENING..ROLLBACK          wallet_id
balance_units  >= 0     status  PENDING..FAILED            transaction_id (fk)
version        >= 1     wallet_id, player_id, currency     direction DEBIT | CREDIT
created_at/updated_at   amount_units                       amount_units > 0
                        provider_id, external_id           balance_before/after
UNIQUE(player,currency) idempotency_key, payload_hash      UNIQUE(wallet_id, transaction_id)
                        round_id, game_id                  append-only (trigger)
                        reference_external_id
                        reference_id (fk)
                        failure_code, result_balance_units
                        attempts, next_attempt_at, expires_at
                        created_at, updated_at, completed_at

inbox_messages                          outbox_events
--------------                          -------------
(consumer_name, message_id) pk          id = eventId (pk)
payload_hash                            aggregate_id, event_type, event_version
received_at, processed_at               correlation_id, causation_id, occurred_at
                                        payload jsonb (snapshot imutável)
                                        attempts, next_attempt_at
                                        locked_by, locked_until (lease)
                                        published_at, last_error
```

Dinheiro: `BIGINT` em centavos + `char(3)` ISO 4217. Nunca float.

## 2. Invariantes financeiras (onde cada uma é garantida)

| Invariante | Banco | Código |
|---|---|---|
| Saldo nunca negativo | `CHECK balance_units >= 0` | `Wallet.Debit` |
| Uma carteira por (jogador, moeda) | `UNIQUE` | conflito 409 |
| Uma abertura por carteira | índice único parcial `kind = 'OPENING'` | - |
| Uma operação por (provider, externalId) | índice único parcial | replay ou 409 |
| Uma chave por provider | índice único parcial | replay ou 409 |
| Um lançamento por (carteira, transação) | `UNIQUE` | - |
| `after = before ± amount` | `CHECK` | `NewLedgerEntry` |
| Ledger imutável | trigger em UPDATE/DELETE/TRUNCATE | - |
| Uma reversão bem-sucedida por referência | índice único parcial | `ALREADY_REVERSED` |
| Estado terminal não muda | trigger | `requireOpen()` |
| Movimento na moeda/jogador da carteira | FK composta `(id, player_id, currency)` | validação |

Regra: o código valida primeiro (mensagem boa); o banco é a rede de segurança
(com 3 instâncias, é o único lugar que vê todo mundo).

## 3. Fronteiras transacionais

Uma operação = **uma transação SQL**:

```
BEGIN
  SELECT wallet ... FOR UPDATE          -- serializa a carteira
  procura duplicata de novo             -- alguém pode ter commitado enquanto eu esperava
  resolve referência (se houver)
  INSERT wager_transactions             -- já no estado final ou PENDING_REFERENCE
  UPDATE wallets ... WHERE version = $v -- se houve movimento
  INSERT wallet_ledger_entries          -- se houve movimento
  INSERT outbox_events                  -- eventos da operação
  INSERT inbox_messages                 -- só quando veio do SQS
COMMIT
```

- Nada é publicado dentro da transação. O relay da outbox publica depois.
- Não existe commit intermediário de `PENDING`: a operação nasce terminal ou
  `PENDING_REFERENCE`. Logo, não existe `PENDING` órfão para recuperar.

## 4. Estratégia de locking

- **Pessimista por carteira**: `SELECT ... FOR UPDATE` na linha da carteira.
  Carteiras diferentes não se bloqueiam (sem lock global).
- **Defesa extra**: `UPDATE ... WHERE id = $1 AND version = $2`. Se afetar 0
  linhas, aborta (nunca deveria acontecer com o lock, mas impede lost update se
  alguém esquecer o lock no futuro).
- Ordem de locks: o worker de pendências trava a linha da transação pendente
  (`SKIP LOCKED`) e depois a carteira; o caminho síncrono (HTTP/SQS) só trava
  carteiras. Como só um dos caminhos pega lock em linha de transação, não há
  ciclo. (O `KEY SHARE` que a FK de uma reversão nova pega na referência não
  conflita com o lock do worker; ver DECISIONS 14.)

Por que não otimista puro? A disputa é exatamente o cenário do teste (mesma
carteira, ao mesmo tempo). Com lock, o segundo espera e decide com o saldo real;
sem lock, vira retry em loop.

## 5. Idempotência

Duas chaves únicas por provider: `idempotency_key` e `external_transaction_id`.

```
achou pela chave?
  hash igual   -> replay (resultado salvo, idempotentReplay=true)
  hash difere  -> 409 IDEMPOTENCY_CONFLICT
não achou pela chave, mas achou pelo externalId (chave diferente)
               -> 409 EXTERNAL_ID_REUSED
não achou      -> processa
```

- Hash: SHA-256 do JSON canônico (chaves ordenadas) dos campos de negócio, sem
  a chave de idempotência. HTTP e SQS montam a mesma `domain.Request`, logo o
  mesmo hash.
- Corrida de duplicatas: as duas passam no lookup, uma ganha o lock; a segunda,
  ao pegar o lock, procura de novo e vira replay. Se ainda assim bater no índice
  único (ex.: carteira diferente na requisição), o erro de unicidade vira replay.
- Replay devolve `result_balance_units` gravado, não o saldo atual.

## 6. Máquina de estados

```
           ┌──────────────► PROCESSED
PENDING ───┼──────────────► REJECTED
           ├──────────────► FAILED
           └► PENDING_REFERENCE ─┬► PROCESSED
                 ▲   (retry)     ├► REJECTED (REFERENCE_NOT_FOUND / REFERENCE_TIMEOUT)
                 └───────────────┘
```

- `PENDING` existe só em memória (operação síncrona).
- `FAILED` existe no domínio, mas o serviço não grava: erro técnico faz
  rollback e o cliente/fila tenta de novo. Ver ARCHITECTURE.md.

## 7. Operações

| Tipo | Efeito | Referência |
|---|---|---|
| BET | débito | - |
| WIN | crédito | opcional (BET da mesma rodada) |
| LOSS | nada (valor 0.00) | - |
| REFUND | crédito igual à BET | obrigatória, BET |
| ROLLBACK | inverso da original | obrigatória, BET/WIN/REFUND |

Referência precisa bater: provider, jogador, carteira, moeda, rodada; reversão
com o mesmo valor. Uma referência aceita **uma** reversão bem-sucedida (REFUND
ou ROLLBACK), garantido por índice único.

## 8. Inbox / Outbox

- **Inbox**: chave `(consumer, messageId)`. Gravada na mesma transação da
  operação. Mensagem só é apagada da fila depois do commit.
- **Outbox**: eventos gravados na mesma transação. Relay em cada instância:
  `claim` com `FOR UPDATE SKIP LOCKED` + lease (`locked_until`), publica no SQS
  FIFO (`group = aggregateId`, `dedup = eventId`), marca publicado. Falha →
  backoff de 1s a 1min, sem limite de tentativas.

## 9. Fluxos de recuperação

| Falha | O que acontece |
|---|---|
| Cai antes do commit | rollback; cliente/fila reenvia; nada foi gravado |
| Cai depois do commit, antes de responder | reenvio vira replay |
| Consumidor cai depois do commit, antes do delete | mensagem volta; inbox reconhece; apaga sem reprocessar |
| Relay cai depois do claim | lease expira; outra instância assume |
| Relay cai depois de publicar, antes de marcar | lease expira; republica com o mesmo eventId |
| Postgres fora | HTTP 503; mensagem fica na fila com backoff |
| SQS fora | outbox acumula; relay tenta com backoff; readiness falha |
| Referência ainda não chegou | `PENDING_REFERENCE`; worker com backoff até o TTL |
| SIGTERM | para HTTP, para de buscar mensagens, termina o que está em mãos ou devolve a visibilidade |

## 10. Pacotes

```
cmd/server            main + subcomando healthcheck
internal/money        value object Money
internal/domain       Wallet, Transaction, LedgerEntry, Request, eventos (sem Fx/HTTP/SQL)
internal/store        Postgres (pgx, SQL explícito)
internal/service      casos de uso: carteiras, operações, pendências, mensagens, outbox
internal/httpapi      rotas, auth OIDC, JSON
internal/queue        SQS: consumidor e publisher
internal/app          composição Fx e workers
internal/config, internal/obs (logs e métricas)
```
