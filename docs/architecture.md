# Arquitetura

## Dois eixos de armazenamento

```
                         ┌─ eixo 1: Provider (PocketBase, dialeto SQLite) ─────────────┐
                         │  coleções de config, schedules, webhooks, produtos, arquivos │
seu código ─► turbinedb ─┤  DBConnect ► provider.Provider ► *dbx.DB (sqlite|mattn|libsql)
   (Config)              └──────────────────────────────────────────────────────────────┘
                         ┌─ eixo 2: SystemDatabase (estado de execução) ───────────────┐
                         │  status, steps, filas, mensagens, eventos, KV                │
                         │  Config.Turbine.SystemDatabase ► turbine.SystemDatabase      │
                         │  nil = SQLite do PocketBase │ providers/postgres = PostgreSQL │
                         └──────────────────────────────────────────────────────────────┘
```

- O eixo 1 funciona com o Turbine original. O eixo 2 exige o fork `feature/pluggable-sysdb` (interface `SystemDatabase` exportada e injetável).
- Com `SystemDatabase` definido, as coleções `pt_*` de execução continuam criadas no PocketBase (migrações, dashboard), mas ficam **vazias**: o estado vive no banco injetado. O dashboard lê pela mesma interface.
- Registro por nome nos dois eixos: `provider.Register` (eixo 1) e `turbinedb.RegisterSystemDatabase` (eixo 2), ambos no `init` do pacote de implementação. `ConfigFromEnv` resolve `TURBINE_DB_PROVIDER` e `TURBINE_SYSDB`.
- O runtime do Turbine chama `Launch`/`Shutdown` do `SystemDatabase`; o adaptador não os chama.

## Camadas

```
seu código ──► turbinedb (Config, NewApp, NewStandalone, ConfigFromEnv)
                 │
                 ├─► PocketBase  core.BaseAppConfig{DBConnect: hook}
                 │       │
                 │       └─► hook(dbPath) ──► Provider.Open(Target{Role, Path}) ──► *dbx.DB
                 │
                 └─► Turbine  turbine.Setup(app) / turbine.SetupStandalone(app)
```

- **`provider/`** — contrato (`Provider`, `Target`, `Role`) e registro por nome. Depende só de `pocketbase/dbx`, para que provedores em módulos separados não puxem o resto.
- **raiz (`turbinedb`)** — monta o app PocketBase com o hook e entrega ao Turbine. Não conhece nenhum driver além do SQLite padrão.
- **`sqlite/`** — provedor padrão (modernc, puro Go). Mesmo módulo da raiz porque o PocketBase já depende desse driver.
- **`providers/*`** — um módulo Go por provedor com driver próprio. `providers/postgres` é o único que implementa o eixo 2 (`turbine.SystemDatabase`) em vez de `provider.Provider`.
- **`sysdb.go` (raiz)** — registro de `SystemDatabaseFactory` (`RegisterSystemDatabase`, `OpenSystemDatabase`), espelho do registro de provedores.

## Dois bancos, dois papéis

O PocketBase abre dois bancos no `Bootstrap`:

| Role | Arquivo padrão | Conteúdo |
|---|---|---|
| `RoleData` | `<DataDir>/data.db` | coleções, registros, **todas** as tabelas `pt_*` do Turbine |
| `RoleAuxiliary` | `<DataDir>/auxiliary.db` | logs de requisição do PocketBase |

O hook recebe só o caminho; `provider.TargetFromPath` deduz o papel pelo nome do arquivo. `Config.AuxProvider` (opcional) permite um provedor diferente para logs; sem ele, `Config.Provider` abre os dois.

Os dois bancos **precisam ser distintos** — cada um tem sua própria tabela `_migrations`.

## Duas conexões por banco

Para cada banco, o PocketBase chama `DBConnect` **duas vezes**: um pool concorrente (leituras) e um pool de conexão única (escritas serializadas). Por isso `Provider.Open` deve devolver um `*dbx.DB` novo a cada chamada.

## Construtor de queries

O PocketBase escolhe o *query builder* do `dbx` pelo nome do driver. Para drivers que não se chamam `sqlite`/`sqlite3` (ex.: `libsql`), o provedor usa `dbx.NewFromDB(sqlDB, "sqlite")` para forçar o builder SQLite.

## Ciclo de vida

**Standalone** (`turbinedb.NewStandalone`):

1. `core.NewBaseApp` com o hook — nada é aberto ainda.
2. `turbine.SetupStandalone(app, cfg.Turbine)`.
3. `Launch()` → `app.Bootstrap()` (abre os 4 pools via provedor, migrações de sistema) → `Runtime.Launch()` (migrações `pt_*`, recuperação, filas, cron de GC).
4. `Shutdown()` → drena o Turbine → `app.ResetBootstrapState()` fecha conexões. Idempotente.

**HTTP** (`turbinedb.NewApp`): `pocketbase.NewWithConfig` com o hook + `turbine.Setup`, que pendura `Launch`/`Shutdown` em `OnServe`/`OnTerminate`. `app.Start()` cuida do resto.

## Registro de provedores

Mesmo padrão de `database/sql`: cada pacote de provedor chama `provider.Register(nome, factory)` no `init`. `ConfigFromEnv` resolve `TURBINE_DB_PROVIDER` via `provider.Open`. Provedor não importado = `ErrUnknownProvider` com a lista dos registrados.

O eixo 2 segue o mesmo padrão: `postgres` chama `turbinedb.RegisterSystemDatabase("postgres", FromSettings)` no `init`; `ConfigFromEnv` lê `TURBINE_SYSDB`, `TURBINE_SYSDB_DSN` e `TURBINE_SYSDB_OPTIONS`, chama `OpenSystemDatabase` (que conecta e migra) e grava o resultado em `cfg.Turbine.SystemDatabase`. Nome desconhecido = `ErrUnknownSystemDatabase` (embrulhado em `ErrInvalidConfig`).
