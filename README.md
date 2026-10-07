# wager-wallet-service

Carteira de apostas em Go: recebe operações de provedores de jogos (HTTP e SQS),
movimenta saldo com ledger append-only, garante idempotência e publica eventos
via outbox. Roda com 3 instâncias atrás do mesmo Postgres.

Antes de escrever código eu desenhei o modelo, as invariantes e as fronteiras
transacionais. Esse desenho está em [`docs/DESIGN.md`](docs/DESIGN.md), e as
decisões, na ordem em que foram tomadas, estão em [`docs/DECISIONS.md`](docs/DECISIONS.md).
Limitações e o que ficou de fora estão em [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Subir

Requisitos: Docker com Compose v2 (Go 1.25 é opcional; sem ele os testes rodam
num container, ver abaixo). `curl` e `jq` para os exemplos.

```bash
cp .env.example .env     # opcional: o compose já tem os mesmos defaults
docker compose up --build -d
docker compose ps        # wallet-1..3 healthy
curl localhost:8081/health/ready
```

Configuração por variável de ambiente; todas estão em [`.env.example`](.env.example)
com o valor padrão. As que mais importam: `DATABASE_URL`, `OIDC_ISSUER` /
`OIDC_JWKS_URL` / `OIDC_AUDIENCE`, `SQS_ENDPOINT`, `SQS_INPUT_URL` /
`SQS_DLQ_URL` / `SQS_EVENTS_URL`, `REFERENCE_RETRY_TTL`, `OUTBOX_LEASE` e
`SHUTDOWN_TIMEOUT`.

Filas: `deploy/localstack/init-sqs.sh` roda no boot do LocalStack e cria
`wager-transactions.fifo` (visibility 30s, redrive para a DLQ após 5
recebimentos), `wager-transactions-dlq.fifo` (retenção 14 dias) e
`wallet-events.fifo`.

Migrations (golang-migrate, em `migrations/`): o serviço `migrate` aplica no
`up`. Para reverter e reaplicar manualmente:

```bash
DB="postgres://wallet:wallet@postgres:5432/wallet?sslmode=disable"
docker compose run --rm migrate -path=/migrations -database "$DB" version
docker compose run --rm migrate -path=/migrations -database "$DB" down -all
docker compose run --rm migrate -path=/migrations -database "$DB" up
```

| Serviço     | Porta host | Observação                                 |
|-------------|-----------:|--------------------------------------------|
| wallet-1..3 | 8081-8083  | mesma imagem, mesma base                   |
| postgres    | 5432       | `wallet` / `wallet`                        |
| keycloak    | 8180       | realm `wager`, importado no boot           |
| localstack  | 4566       | filas FIFO criadas por `deploy/localstack` |

As migrations rodam no serviço `migrate` (golang-migrate) antes das instâncias.

## Usar

Tokens via client credentials (os secrets do realm de dev são `<client>-secret`):

```bash
OP=$(curl -s -d grant_type=client_credentials -d client_id=wallet-internal -d client_secret=wallet-internal-secret \
  localhost:8180/realms/wager/protocol/openid-connect/token | jq -r .access_token)
PA=$(curl -s -d grant_type=client_credentials -d client_id=provider-a -d client_secret=provider-a-secret \
  localhost:8180/realms/wager/protocol/openid-connect/token | jq -r .access_token)

# abrir carteira (serviço interno)
curl -s localhost:8081/wallets -H "Authorization: Bearer $OP" -H 'Content-Type: application/json' \
  -d '{"playerId":"8f6c1d1e-7f43-4b8e-9d0a-0d6c3b1d2a10","initialBalance":{"amount":"100.00","currency":"BRL"}}'

# aposta (provedor)
curl -s localhost:8082/wagering/transactions -H "Authorization: Bearer $PA" -H 'Idempotency-Key: bet-1' \
  -H 'Content-Type: application/json' -d '{
    "providerId":"provider-a","externalTransactionId":"bet-1","playerId":"<playerId>","walletId":"<walletId>",
    "roundId":"r-1","gameId":"g-1","kind":"BET","money":{"amount":"30.00","currency":"BRL"}}'
```

| Rota | Quem | |
|---|---|---|
| `POST /wallets` | operator | abre carteira; saldo inicial > 0 vira transação `OPENING` |
| `GET /wallets/{id}` | operator | |
| `GET /wallets/{id}/ledger?limit=&cursor=` | operator | paginação por cursor opaco |
| `POST /wallets/{id}/reconciliation` | operator | saldo armazenado vs. soma do ledger |
| `POST /wagering/transactions` | provider | exige `Idempotency-Key`; 201 / 200 replay / 202 pendente / 422 rejeitada |
| `GET /wagering/transactions/{id}` | provider (só as suas) ou operator | |
| `GET /providers/{p}/wagering/transactions/{externalId}` | provider `p` ou operator | |
| `GET /health/live`, `/health/ready`, `/metrics` | público | |

Erros: `{"code": "...", "message": "..."}`. O `providerId` do corpo tem que bater
com a claim `provider_id` do token, senão 403.

Entrada por fila: `wager-transactions.fifo`, mensagem
`{"messageId","type":"WagerTransactionRequested","occurredAt","data":{...mesmo corpo do HTTP..., "idempotencyKey"}}`.
Eventos saem em `wallet-events.fifo`.

## Testes

Com Go instalado, na raiz:

```bash
go vet ./...
go test ./...            # unitários (integração e multi-instância têm build tag)
go test -race ./...
```

Os testes de integração precisam do compose no ar (`docker compose up -d`):
usam o Postgres dele criando um banco temporário por execução, o Keycloak
importado e filas próprias criadas no LocalStack. Sem Go instalado, tudo roda
num container na rede do compose:

```bash
# unitários (domínio, dinheiro, decode de mensagens)
docker run --rm -v "$PWD:/src" -w /src golang:1.25 go test -race ./internal/...

# integração: Postgres real (banco temporário + migrations), Keycloak, LocalStack
docker run --rm -v "$PWD:/src" -w /src --network wager-wallet-service_default \
  -e DATABASE_URL=postgres://wallet:wallet@postgres:5432/wallet?sslmode=disable \
  -e KEYCLOAK_URL=http://keycloak:8180 -e SQS_ENDPOINT=http://localstack:4566 \
  golang:1.25 go test -race -count=1 -tags=integration ./test/integration/...

# 3 instâncias do compose disputando as mesmas carteiras
docker run --rm -v "$PWD:/src" -w /src --network wager-wallet-service_default \
  -e KEYCLOAK_URL=http://keycloak:8180 -e WALLET_URLS=http://wallet-1:8080,http://wallet-2:8080,http://wallet-3:8080 \
  golang:1.25 go test -count=1 -tags=multiinstance ./test/multiinstance/...
```

Cenários obrigatórios cobertos: duas apostas de 80 sobre saldo 100 (uma passa,
uma `INSUFFICIENT_FUNDS`), a mesma aposta 50 vezes (um débito), carteiras
distintas em paralelo, reentrega SQS depois do commit, DLQ, outbox com dois
relays e lease expirada, 401/403, isolamento entre provedores, reinício da app
preservando estado.

Simulando falhas no ambiente do compose:

```bash
docker compose stop postgres      # /health/ready -> 503; operações -> 503 SERVICE_UNAVAILABLE
docker compose start postgres     # instâncias voltam sozinhas, nada foi gravado pela metade
docker compose stop localstack    # ready -> 503 (sqs DOWN), HTTP segue; eventos acumulam na outbox
docker compose start localstack   # filas recriadas pelo init, relay publica o atraso (LocalStack sem persistência perde o que estava na fila)
docker compose kill wallet-2      # as outras instâncias seguem; leases e mensagens dela expiram e são retomadas
```
