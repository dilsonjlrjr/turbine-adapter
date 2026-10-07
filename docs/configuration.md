# Configuração

## Struct `turbinedb.Config`

O valor zero é válido: SQLite local em `./pb_data`.

| Campo | Padrão | Descrição |
|---|---|---|
| `Provider` | `sqlite.New(sqlite.Config{})` | Abre o banco de dados (e o auxiliar, se `AuxProvider` for nil) |
| `AuxProvider` | `= Provider` | Provedor só para o banco auxiliar (logs) |
| `DataDir` | `pb_data` | Diretório do PocketBase: arquivos locais, uploads, backups |
| `Dev` | `false` | Modo dev do PocketBase (loga SQL) |
| `EncryptionEnv` | — | Nome da env com a chave de criptografia de settings do PocketBase |
| `QueryTimeout` | 30s (PocketBase) | Timeout por query |
| `DataMaxOpenConns` / `DataMaxIdleConns` | 120 / 15 (PocketBase) | Pool do banco de dados |
| `AuxMaxOpenConns` / `AuxMaxIdleConns` | 20 / 3 (PocketBase) | Pool do banco auxiliar |
| `Turbine` | — | `turbine.Config` repassado sem alteração |

Valores negativos em timeout/pools retornam `ErrInvalidConfig`.

## Variáveis de ambiente (`ConfigFromEnv`)

| Variável | Campo | Exemplo |
|---|---|---|
| `TURBINE_DB_PROVIDER` | `Provider` (nome no registro) | `sqlite`, `libsql`, `mattn` |
| `TURBINE_DB_DSN` | `Settings.DSN` | `libsql://db.turso.io` |
| `TURBINE_DB_AUTH_TOKEN` | `Settings.AuthToken` | JWT |
| `TURBINE_DB_OPTIONS` | `Settings.Options` | `aux_url=http://h:8081,aux_auth_token=x` |
| `TURBINE_DB_AUX_PROVIDER` | `AuxProvider` | `sqlite` |
| `TURBINE_DB_AUX_DSN` / `_AUTH_TOKEN` / `_OPTIONS` | idem para o auxiliar | |
| `TURBINE_DATA_DIR` | `DataDir` | `/var/lib/app` |
| `TURBINE_DEV` | `Dev` | `true` |
| `TURBINE_DB_QUERY_TIMEOUT` | `QueryTimeout` | `10s` |
| `TURBINE_EXECUTOR_ID` | `Turbine.ExecutorID` | `worker-1` |
| `TURBINE_APP_VERSION` | `Turbine.ApplicationVersion` | `v1.4.0` |
| `TURBINE_GC_RETENTION` | `Turbine.GCRetention` | `168h` (negativo desliga) |
| `TURBINE_SHUTDOWN_TIMEOUT` | `Turbine.ShutdownTimeout` | `30s` |
| `TURBINE_SYSDB` | `Turbine.SystemDatabase` (nome no registro; vazio = SQLite embutido) | `postgres` |
| `TURBINE_SYSDB_DSN` | `Settings.DSN` do banco de execução | `postgres://u:p@host:5432/db?sslmode=disable` |
| `TURBINE_SYSDB_OPTIONS` | `Settings.Options` do banco de execução | `schema=turbine,max_conns=20,poll_interval=500ms` |

`TURBINE_DB_OPTIONS` usa `chave=valor` separados por vírgula; o valor pode conter `=`.

Provedores diferentes de `sqlite` precisam estar linkados no binário:

```go
import _ "github.com/turbine-adapter/turbinedb/providers/libsql"
```

`TURBINE_SYSDB` exige o fork do Turbine (`feature/pluggable-sysdb`) e o import em branco da implementação:

```go
import _ "github.com/turbine-adapter/turbinedb/providers/postgres"
```

Ao ler `TURBINE_SYSDB`, `ConfigFromEnv` já **conecta e migra** o banco. Erros de nome desconhecido, opção inválida ou conexão voltam como `ErrInvalidConfig`.

Para outra fonte (Vault, arquivo, flags) use `turbinedb.ConfigFromLookup(func(key string) (string, bool))`.

## Opções por provedor

| Provedor | DSN | AuthToken | Options |
|---|---|---|---|
| `sqlite` | ignorado | ignorado | `pragmas` — lista separada por `;` que substitui os padrões |
| `mattn` | ignorado | ignorado | qualquer parâmetro DSN do go-sqlite3 (`_busy_timeout=5000`) |
| `libsql` | URL do banco de dados (obrigatório) | JWT | `aux_url`, `aux_auth_token` |

## Opções por `SystemDatabase` (`TURBINE_SYSDB_*`)

| Implementação | DSN | Options |
|---|---|---|
| `postgres` | `postgres://user:pass@host:5432/db?sslmode=disable` (obrigatório) | `schema` (padrão `turbine`), `max_conns` (inteiro), `poll_interval` (duração, padrão `1s`) |

Detalhes e semântica em [providers/postgres/README.md](../providers/postgres/README.md). Registro por código: `turbinedb.RegisterSystemDatabase(name, factory)` e `turbinedb.OpenSystemDatabase(ctx, name, settings)`.
