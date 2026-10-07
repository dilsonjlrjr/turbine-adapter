# providers/postgres

Estado de execução do Turbine em **PostgreSQL**, implementando `turbine.SystemDatabase` sobre `github.com/jackc/pgx/v5` (`pgxpool`).

Diferente dos demais provedores (`sqlite`, `mattn`, `libsql`), este **não** é um `provider.Provider` do PocketBase: ele é plugado em `turbine.Config.SystemDatabase` e exige o fork `feature/pluggable-sysdb` do Turbine (ver [docs/viability.md](../../docs/viability.md)).

```go
import (
	"github.com/YakirOren/turbine"
	"github.com/turbine-adapter/turbinedb"
	"github.com/turbine-adapter/turbinedb/providers/postgres"
)

db, err := postgres.Open(ctx, postgres.Config{
	DSN: "postgres://user:pass@host:5432/app?sslmode=disable",
})
if err != nil { ... }

s, _ := turbinedb.NewStandalone(turbinedb.Config{
	Turbine: turbine.Config{SystemDatabase: db},
})
defer s.Shutdown()
```

O `Runtime` do Turbine chama `Launch` e `Shutdown` no banco; **não chame você mesmo**. `Shutdown` para o listener e fecha o pool quando ele foi criado por `Open` (com `New`, o pool é seu e não é fechado).

## O que fica em Postgres e o que fica no PocketBase

| Postgres (`<schema>.*`) | PocketBase (SQLite ou provedor SQLite-dialeto) |
|---|---|
| `workflow_status` — status, entradas/saídas, filas, tentativas | coleções de configuração: schedules, webhooks, canais de alerta, workflows registrados |
| `operation_outputs` — checkpoints de steps | `pt_products` e arquivos enviados (uploads) |
| `notifications` — mensagens `Send`/`Recv` | contas, settings, logs e demais coleções do PocketBase |
| `workflow_events` e `workflow_events_history` | migrações `pt_*` (as coleções existem, mas ficam **vazias** de estado de execução) |
| `kv` — armazenamento chave/valor | |

O PocketBase continua exigindo dialeto SQLite; combine este módulo com `sqlite`, `mattn` ou `libsql` em `Config.Provider` conforme a necessidade.

## Configuração (`postgres.Config`)

| Campo | Padrão | Descrição |
|---|---|---|
| `DSN` | — (obrigatório em `Open`) | `postgres://user:pass@host:5432/db?sslmode=disable` |
| `Schema` | `turbine` | Schema onde ficam todas as tabelas; criado se não existir. Identificador simples (`[A-Za-z_][A-Za-z0-9_]*`, até 48 caracteres) |
| `MaxConns` | padrão do pgx | Máximo de conexões do pool criado por `Open`. Uma conexão fica retida pelo `LISTEN` |
| `PollInterval` | `1s` | Reverificação periódica de `Recv`, `GetEvent`, `AwaitWorkflowResult` e do canal de `WaitForEnqueue` |
| `Logger` | `slog.Default()` | Logger opcional |

`postgres.New(pool, cfg)` aceita um `*pgxpool.Pool` seu (precisa de pelo menos 2 conexões); `DSN` e `MaxConns` são ignorados.

## Variáveis de ambiente

Com `import _ "github.com/turbine-adapter/turbinedb/providers/postgres"` o pacote se registra como `postgres`:

```bash
TURBINE_SYSDB=postgres \
TURBINE_SYSDB_DSN='postgres://user:pass@host:5432/app?sslmode=disable' \
TURBINE_SYSDB_OPTIONS='schema=turbine,max_conns=20,poll_interval=500ms' \
./meu-servico
```

| Opção (`TURBINE_SYSDB_OPTIONS`) | Campo |
|---|---|
| `schema` | `Config.Schema` |
| `max_conns` | `Config.MaxConns` |
| `poll_interval` | `Config.PollInterval` (duração Go) |

`turbinedb.ConfigFromEnv()` abre o banco (conecta e migra) e preenche `cfg.Turbine.SystemDatabase`.

## LISTEN/NOTIFY e fallback por polling

- Cada instância mantém um registro de waiters em memória com as chaves do contrato: `queue::<fila>`, `workflow::<id>` e `<workflow>::<topic|key>`.
- Toda escrita que deve acordar alguém executa `pg_notify('<schema>_turbine', <chave>)` **na mesma transação** da escrita (entregue no commit). Uma conexão dedicada em `LISTEN` repassa as notificações aos waiters locais, em qualquer processo.
- O listener reconecta com backoff exponencial (100 ms a 5 s) e, após reconectar, acorda todos os waiters (notificações podem ter sido perdidas).
- Fallback: `Recv`, `GetEvent` e `AwaitWorkflowResult` revalidam a cada `PollInterval`; o canal de `WaitForEnqueue` também dispara após `PollInterval` (cobre expiração de rate limit e notificação perdida). Consequência: cada fila consulta o banco ao menos uma vez por `PollInterval`.
- Payload do `NOTIFY` limitado a ~8 KB: chaves maiores não são enviadas e dependem do polling.

## Várias instâncias

- Qualquer número de processos pode apontar para o mesmo schema.
- `Recv` usa `FOR UPDATE SKIP LOCKED`: cada mensagem vai a um único receptor.
- `DequeueWorkflows` com limites (worker, global, rate) toma `pg_advisory_xact_lock` por (schema, fila, partição) e faz contagem e claim na mesma transação, então os limites são exatos entre processos. Sem limites, apenas `SKIP LOCKED`.
- Relógios: `updated_at` e a janela do rate limit usam o relógio da instância que escreve; mantenha NTP nos nós.

## Migrações

SQL versionado e embutido (`migrations/NNNN_nome.sql`, `embed`). A tabela `<schema>.schema_migrations` registra as versões aplicadas. `Open`/`New` aplicam as pendentes sob `pg_advisory_lock`, então vários processos subindo juntos são serializados. Migrações rodam com `search_path` do schema configurado; as queries em runtime usam nomes totalmente qualificados.

Esquema: `workflow_status`, `operation_outputs` (FK com `ON DELETE CASCADE` para `workflow_status`), `notifications`, `workflow_events`, `workflow_events_history`, `kv`, com índices únicos (dedup parcial, `(workflow_id, function_id)`, `(workflow_id, key)`, `kv.key`) e de desempenho (fila/status, partição, executor, `created_at`, mensagens não consumidas).

## Desenvolvimento local

Os testes usam, nesta ordem:

1. `TURBINEDB_POSTGRES_DSN`, se definida (qualquer Postgres 14+; cada teste cria e remove seu próprio schema);
2. caso contrário, um Postgres embutido (`fergusstrange/embedded-postgres`, porta livre, diretório temporário; baixa os binários na primeira execução).

```bash
docker run --rm -e POSTGRES_PASSWORD=pg -p 5432:5432 postgres:17
TURBINEDB_POSTGRES_DSN='postgres://postgres:pg@127.0.0.1:5432/postgres?sslmode=disable' make test-postgres
```

A suíte de conformidade do Turbine (`sysdbtest.RunSuite`) roda inteira contra este módulo.

## Limites

- `notifications`, `workflow_events` e `workflow_events_history` **não têm chave estrangeira**: o contrato permite `Send`/`SetEvent` para um workflow que ainda não existe. A limpeza é explícita em `DeleteWorkflow` e `GarbageCollectWorkflows`; eventos/mensagens órfãos permanecem até existirem e serem removidos com o workflow.
- Payloads são `TEXT` e não podem conter o byte `NUL` (limitação do Postgres).
- Ordenação por `name` e `status` usa collation `C` (binária), como o SQLite.
- Pgbouncer em modo *transaction*: `LISTEN` e `pg_advisory_lock` de sessão exigem conexões diretas ou modo *session*.
- O PocketBase ainda precisa de um banco SQLite-dialeto para coleções de configuração, produtos e arquivos.
