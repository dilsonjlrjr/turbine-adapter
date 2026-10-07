# turbinedb

Adaptador de provedores de banco de dados para o [Turbine](https://github.com/YakirOren/turbine) — motor de workflows duráveis em Go construído sobre PocketBase.

O Turbine grava tudo em SQLite local (`pb_data/data.db`). O `turbinedb` deixa você trocar **onde e como** esse banco é aberto — libSQL/Turso remoto, SQLite via cgo, SQLite com pragmas próprios — **sem fork** e sem alterar uma linha do Turbine. Com o fork `feature/pluggable-sysdb` do Turbine, o **estado de execução** dos workflows também pode ir para **PostgreSQL**.

## Dois eixos de armazenamento

| Eixo | O que guarda | Como se troca | Dialeto |
|---|---|---|---|
| **Provider** (PocketBase) | coleções de configuração, schedules, webhooks, produtos, arquivos, contas, logs | `Config.Provider` / `TURBINE_DB_PROVIDER` | SQLite (sempre) |
| **SystemDatabase** (execução) | status de workflows, steps, filas, mensagens, eventos, KV | `Config.Turbine.SystemDatabase` / `TURBINE_SYSDB` | qualquer (ex.: PostgreSQL) |

Os dois são independentes: dá para usar SQLite local no PocketBase e Postgres para a execução, ou libSQL no PocketBase e o SQLite embutido na execução (padrão).

```go
p, _ := libsql.New(libsql.Config{URL: "libsql://minha-db.turso.io", AuthToken: token})

s, _ := turbinedb.NewStandalone(turbinedb.Config{Provider: p})
defer s.Shutdown()

turbine.Register(s.Runtime, MeuWorkflow)
s.Launch()
```

## Por que funciona sem fork

O Turbine depende de `core.App` do PocketBase e aceita um app pronto em `turbine.Setup` / `turbine.SetupStandalone`. O PocketBase, por sua vez, expõe um único ponto de troca de armazenamento: o hook `DBConnect`. O adaptador constrói o app com esse hook apontando para um `Provider` e o entrega ao Turbine. Detalhes em [docs/architecture.md](docs/architecture.md).

**Restrição:** o provedor do PocketBase precisa falar **dialeto SQLite**. PostgreSQL/MySQL no lugar do PocketBase não são viáveis — mas o estado de execução do Turbine pode ir para Postgres via `SystemDatabase` (requer o fork). Ver [docs/viability.md](docs/viability.md).

## Provedores

| Provedor | Eixo | Pacote / módulo | cgo | Uso |
|---|---|---|---|---|
| `sqlite` (padrão) | Provider | `github.com/turbine-adapter/turbinedb/sqlite` | não | Arquivo local, pragmas configuráveis |
| `mattn` | Provider | `github.com/turbine-adapter/turbinedb/providers/mattn` | **sim** | Arquivo local via `mattn/go-sqlite3` |
| `libsql` | Provider | `github.com/turbine-adapter/turbinedb/providers/libsql` | não | Turso / sqld remoto |
| `postgres` | SystemDatabase | `github.com/turbine-adapter/turbinedb/providers/postgres` | não | Estado de execução em PostgreSQL (LISTEN/NOTIFY, multi-instância) |

Cada provedor com dependência própria vive em **módulo Go separado**: você só baixa o driver que usa.

## Instalação

```bash
go get github.com/turbine-adapter/turbinedb
go get github.com/turbine-adapter/turbinedb/providers/libsql   # opcional
go get github.com/turbine-adapter/turbinedb/providers/postgres # opcional (exige o fork do Turbine)
```

## Três formas de configurar

1. **Explícita** — instancie o provedor e passe em `Config.Provider`.
2. **Por nome** — `provider.Open("libsql", provider.Settings{DSN: url, AuthToken: tok})`.
3. **Por ambiente** — `turbinedb.ConfigFromEnv()` + import em branco do provedor:

```bash
TURBINE_DB_PROVIDER=libsql \
TURBINE_DB_DSN=libsql://minha-db.turso.io \
TURBINE_DB_AUTH_TOKEN=... \
./meu-servico
```

Estado de execução em Postgres, por ambiente:

```bash
TURBINE_SYSDB=postgres \
TURBINE_SYSDB_DSN='postgres://user:pass@host:5432/app?sslmode=disable' \
TURBINE_SYSDB_OPTIONS='schema=turbine,max_conns=20' \
./meu-servico   # com import _ ".../providers/postgres"
```

Ou em código: `turbinedb.OpenSystemDatabase(ctx, "postgres", settings)` / `postgres.Open(ctx, postgres.Config{...})` e `Config.Turbine.SystemDatabase = db`.

Referência completa de variáveis: [docs/configuration.md](docs/configuration.md).

## Modos de execução

| Turbine original | turbinedb | Quando usar |
|---|---|---|
| `turbine.NewStandalone(cfg)` | `turbinedb.NewStandalone(cfg)` | Workers, scripts, sem HTTP |
| `turbine.NewApp(cfg)` | `turbinedb.NewApp(cfg)` | Servidor PocketBase + dashboard |

`Config.Turbine` repassa `turbine.Config` sem alteração.

## Mapa da documentação

| Contexto | Arquivo |
|---|---|
| Arquitetura e fluxo de conexão | [docs/architecture.md](docs/architecture.md) |
| Análise de viabilidade | [docs/viability.md](docs/viability.md) |
| Configuração (struct + env) | [docs/configuration.md](docs/configuration.md) |
| Desenvolvimento, testes, lint | [docs/development.md](docs/development.md) |
| Contrato `Provider` e como escrever um novo | [provider/README.md](provider/README.md) |
| Provedor SQLite padrão | [sqlite/README.md](sqlite/README.md) |
| Provedor mattn (cgo) | [providers/mattn/README.md](providers/mattn/README.md) |
| Provedor libSQL / Turso | [providers/libsql/README.md](providers/libsql/README.md) |
| SystemDatabase PostgreSQL | [providers/postgres/README.md](providers/postgres/README.md) |
| Exemplos executáveis | [examples/README.md](examples/README.md) |
