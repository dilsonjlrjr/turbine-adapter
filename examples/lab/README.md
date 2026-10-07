# lab

Aplicação completa de **processamento de pedidos** sobre PostgreSQL: o estado de execução do Turbine fica no Postgres (`TURBINE_SYSDB=postgres`) e o PocketBase usa o SQLite local (volume `/pb_data`). Toda a configuração vem de variáveis de ambiente via `turbinedb.ConfigFromEnv()`.

## O que demonstra

| Recurso | Onde |
|---|---|
| Workflow disparável pelo dashboard, com schema de entrada, tags e resumo | `ProcessOrder` (`main.go`) |
| Passos nomeados: `validate`, `reserve-stock`, `charge-payment`, `ship`, `invoice` | `orders.go` |
| Retry com backoff exponencial (cobrança falha ~30% das vezes) | `charge-payment` |
| Aprovação humana (botão *Approve* no dashboard) acima de `approval_threshold` | `approveIfNeeded` |
| Sleep durável (`turbine.Pause`) | etapa de envio |
| Produto (fatura `.txt` anexada ao workflow) e `ProductSender` | `generateInvoice`, `logSender` |
| Status/cor do app e eventos (`SetAppStatus`, `SetValue`) | `stage` |
| Workflow filho (`NotifyCustomer`, `PaymentGateway`) com ID determinístico | `helpers.go` |
| `Send`/`Recv` entre workflows (gateway responde ao pedido) | `settleWithGateway` |
| Fila `orders` com concorrência por worker/global e partição por cliente | `registerWorkflows` |
| Workflow agendado (cron a cada 2 min) que enfileira pedidos aleatórios | `GenerateDemoOrders` |
| KV: `approval_threshold` (config) e `orders_processed` (contador) | `seedKV`, `countProcessed` |
| Superusuário criado/atualizado no start | `upsertSuperuser` |

Não usado: a API do fork não expõe relação pai/filho explícita na criação de workflows (um "filho" é apenas um `Run` dentro de um passo), então o vínculo é feito pelo ID `<pedido>-gateway` / `<pedido>-notify`.

## Variáveis de ambiente

| Variável | Descrição |
|---|---|
| `TURBINE_SYSDB` | `postgres` |
| `TURBINE_SYSDB_DSN` | DSN do Postgres (no compose é montado a partir de `POSTGRES_PASSWORD`) |
| `TURBINE_SYSDB_OPTIONS` | ex.: `schema=turbine,max_conns=20,poll_interval=500ms` |
| `TURBINE_EXECUTOR_ID`, `TURBINE_APP_VERSION`, `TURBINE_GC_RETENTION` | ver [docs/configuration.md](../../docs/configuration.md) |
| `TURBINE_DATA_DIR` | diretório do PocketBase (padrão `pb_data`) |
| `LAB_SUPERUSER_EMAIL`, `LAB_SUPERUSER_PASSWORD` | superusuário do PocketBase (opcionais) |
| `POSTGRES_PASSWORD`, `APP_PORT` | só para o compose (`APP_PORT` padrão `28090`) |

## Rodar localmente

Com qualquer Postgres acessível:

```bash
cd examples
export TURBINE_SYSDB=postgres \
  TURBINE_SYSDB_DSN='postgres://user:pass@127.0.0.1:5432/db?sslmode=disable' \
  LAB_SUPERUSER_EMAIL=admin@turbine.lab LAB_SUPERUSER_PASSWORD=troque-esta-senha
go run ./lab serve --http=127.0.0.1:8090
```

O log imprime a URL do dashboard. Para um Postgres descartável: `docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=pass postgres:17`.

## Docker Compose

```bash
cd examples/lab/deploy
cp .env.example .env   # ajuste as senhas; .env nunca é versionado
docker compose up -d --build
```

O Postgres não é publicado no host. Se o repositório estiver copiado em outro layout (ex.: `./src` ao lado do compose), defina `BUILD_CONTEXT=./src` no `.env`.

## URLs

- Dashboard: `http://localhost:28090/_/turbine/`
- Admin do PocketBase: `http://localhost:28090/_/`
- Saúde: `http://localhost:28090/api/health`
- API do dashboard (autenticada como superusuário): `/api/pt/workflows`, `/api/pt/trigger`, `/api/pt/workflows/{id}/approve`

Disparar um pedido pela API:

```bash
TOKEN=$(curl -s -X POST localhost:28090/api/collections/_superusers/auth-with-password \
  -H 'content-type: application/json' \
  -d '{"identity":"admin@turbine.lab","password":"..."}' | jq -r .token)
curl -X POST localhost:28090/api/pt/trigger -H "Authorization: $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"workflow_fqn":"main.ProcessOrder","input":{"order_id":"ORD-1","customer":"acme","amount":900,"items":2}}'
```

Pedidos acima de R$ 500 (`approval_threshold` no KV) ficam em *waiting for approval* até serem aprovados no dashboard.
