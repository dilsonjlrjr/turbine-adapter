# examples

Módulo separado com binários executáveis.

| Exemplo | Provedor | Modo | Rodar |
|---|---|---|---|
| [standalone](standalone/main.go) | `sqlite` com pragma extra | standalone | `go run ./standalone` |
| [fromenv](fromenv/main.go) | escolhido por `TURBINE_DB_PROVIDER` | standalone | `go run ./fromenv` |
| [httpapp](httpapp/main.go) | `libsql` | HTTP (PocketBase + dashboard) | `LIBSQL_URL=... go run ./httpapp serve` |
| [lab](lab/README.md) | PocketBase local + execução em `postgres` | HTTP completo (pedidos, aprovação, filas, cron) | veja o README |

`fromenv` com libSQL local:

```bash
docker run --rm -p 8080:8080 ghcr.io/tursodatabase/libsql-server:latest
TURBINE_DB_PROVIDER=libsql TURBINE_DB_DSN=http://127.0.0.1:8080 go run ./fromenv
```

Arquivos locais vão para `./pb_data` (ignorado pelo git).
