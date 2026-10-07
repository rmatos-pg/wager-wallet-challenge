# Decisões, na ordem em que apareceram

Registro curto do raciocínio. Cada item diz o que eu decidi, o que descartei
e por quê. A ordem é a ordem real: primeiro o papel, depois o banco, depois o
código, e por último o que os testes me obrigaram a mudar.

## Antes do código

**1. Dinheiro é `int64` em centavos + moeda.**
Descartei `float64` (óbvio) e `decimal` com biblioteca externa. Centavos
inteiros cobrem o domínio, mapeiam direto para `BIGINT` e permitem detectar
overflow. A entrada externa só aceita a forma canônica (`"10.00"`, duas casas,
sem sinal) para não existir duas strings com o mesmo valor e hashes diferentes.

**2. Uma operação = uma transação SQL.**
Era a decisão que destravava todo o resto. Se a transação de aposta, o
lançamento, o novo saldo, os eventos e a inbox entram no mesmo commit, não
existe estado parcial para recuperar e não preciso de saga/compensação.
Consequência: `PENDING` nunca é gravado. A operação nasce terminal
(`PROCESSED`/`REJECTED`) ou `PENDING_REFERENCE`.

**3. Lock pessimista na carteira, versão como cinto de segurança.**
Considerei só otimista (versão + retry). Sob disputa na mesma carteira (o
cenário 80/80 e o de 50 iguais) isso vira tempestade de retries. Fiquei com
`SELECT ... FOR UPDATE` na linha da carteira, que serializa por carteira e não
trava carteiras diferentes, e mantive o `UPDATE ... WHERE version = $prev`
para pegar qualquer caminho que esqueça o lock.

**4. Idempotência no banco, não em memória/cache.**
Três instâncias: qualquer dedupe em memória falha. Índices únicos
`(provider, idempotency_key)` e `(provider, external_id)`. Mesma chave com
payload diferente é conflito (409), não replay. O hash é SHA-256 do JSON
canônico (chaves ordenadas) dos campos de negócio, igual para HTTP e SQS. Não
há etapa de normalização: como o parser só aceita a forma canônica do valor,
o mesmo pedido sempre gera o mesmo hash.

**5. Replay devolve o resultado original.**
Guardo o saldo resultante na transação. Reenviar uma aposta antiga devolve o
saldo daquele momento, não o atual; senão o provedor veria a "mesma" resposta
mudar.

**6. Reversão antes da referência espera, não rejeita.**
Na prática REFUND/ROLLBACK podem chegar antes da aposta (filas diferentes,
retries). Rejeitar na hora perderia dinheiro do jogador. Fica
`PENDING_REFERENCE` com backoff exponencial e TTL; ao expirar vira `REJECTED`
com motivo. Uma reversão bem-sucedida por referência, garantida por índice
único parcial.

**7. Inbox na mesma transação; outbox com lease.**
Inbox separada da tabela de transações porque a chave da fila é o
`messageId`, não a chave de idempotência, e um `messageId` reaproveitado com
conteúdo diferente precisa ir para a DLQ. Outbox: claim com
`FOR UPDATE SKIP LOCKED` + lease, para várias instâncias publicarem sem
coordenação e uma instância morta não segurar eventos para sempre.

**8. O banco também defende as invariantes.**
`CHECK` de saldo não negativo, aritmética do lançamento, FK composta
carteira/jogador/moeda, triggers de append-only no ledger e de imutabilidade
em transação terminal. O código já garante tudo isso; o banco garante contra
o próximo script manual ou bug.

## Durante a implementação

**9. Poucas camadas.**
Comecei a escrever interfaces de repositório e parei: com um único banco e
testes contra Postgres real, eram só indireção. O service usa o `store`
direto. Domínio continua sem dependência de banco ou HTTP.

**10. FIFO: falhou uma do grupo, as seguintes do lote esperam.**
O consumidor recebe até 10 mensagens. Se uma mensagem de um grupo falha de
forma transitória e eu sigo processando as próximas do mesmo grupo, quebro a
ordem por carteira. As seguintes voltam com visibilidade 0.

**11. Erro permanente vai para a DLQ na hora; transitório usa o redrive.**
Mensagem inválida, carteira inexistente, conflito de idempotência nunca vão
passar: copio para a DLQ com `deadLetterReason` e apago. Erro de
infraestrutura deixa a mensagem na fila com visibilidade crescente e o
`maxReceiveCount=5` cuida do resto.

**12. Shutdown em ordem.**
HTTP para primeiro, depois os workers (contexto de poll cancela na hora,
contexto de trabalho vive até o deadline), pool por último. Registrar os
workers antes do HTTP no Fx dá essa ordem de graça (stop é LIFO).

**13. Logger do Fx no nível raiz.**
`fx.WithLogger` dentro de um módulo só vale para aquele módulo, e os logs de
start/stop do Fx saem em texto puro no meio do JSON. Já conhecia essa armadilha,
então ficou na raiz desde o início; o teste de ciclo de vida confirma.

## O que os testes mudaram

**14. Worker de pendências: `FOR NO KEY UPDATE`, não `FOR UPDATE`.**
O worker trava a linha da transação pendente com `SKIP LOCKED`. Com
`FOR UPDATE`, inserir uma reversão que referencia essa linha (FK pega
`KEY SHARE`) ficava esperando. `FOR NO KEY UPDATE` não conflita com
`KEY SHARE` e é suficiente, porque o worker não muda a chave.

**15. Banco temporário por execução de teste.**
Os testes de integração criam um banco novo, aplicam as mesmas migrations do
compose e apagam no fim. Assim o teste de migration up/down/up roda de
verdade e os testes nunca sujam a base das instâncias.

**16. Readiness com prazo por dependência.**
Simulando a queda do Postgres no compose, o `/health/ready` marcava também o
SQS como fora: os dois checks dividiam o mesmo timeout de 2s e o do banco
consumia tudo. Cada check passou a ter o próprio prazo, para o diagnóstico
apontar a dependência certa.
