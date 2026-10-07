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

`TURBINE_DB_OPTIONS` usa `chave=valor` separados por vírgula; o valor pode conter `=`.

Provedores diferentes de `sqlite` precisam estar linkados no binário:

```go
import _ "github.com/turbine-adapter/turbinedb/providers/libsql"
```

Para outra fonte (Vault, arquivo, flags) use `turbinedb.ConfigFromLookup(func(key string) (string, bool))`.

## Opções por provedor

| Provedor | DSN | AuthToken | Options |
|---|---|---|---|
| `sqlite` | ignorado | ignorado | `pragmas` — lista separada por `;` que substitui os padrões |
| `mattn` | ignorado | ignorado | qualquer parâmetro DSN do go-sqlite3 (`_busy_timeout=5000`) |
| `libsql` | URL do banco de dados (obrigatório) | JWT | `aux_url`, `aux_auth_token` |
