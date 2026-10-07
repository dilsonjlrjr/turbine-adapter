# providers/libsql

Bancos [libSQL](https://github.com/tursodatabase/libsql) remotos — Turso ou `sqld` self-hosted — via cliente puro Go [`libsql-client-go`](https://github.com/tursodatabase/libsql-client-go). Módulo separado.

```bash
go get github.com/turbine-adapter/turbinedb/providers/libsql
```

```go
p, err := libsql.New(libsql.Config{
    URL:       "libsql://minha-db-org.turso.io",
    AuthToken: os.Getenv("TURSO_TOKEN"),
})
s, err := turbinedb.NewStandalone(turbinedb.Config{Provider: p})
```

## Onde fica cada banco

| Banco | Destino |
|---|---|
| Dados (coleções + `pt_*` do Turbine) | `URL` |
| Auxiliar (logs PocketBase) | `AuxURL` se definido; senão SQLite local em `<DataDir>/auxiliary.db` |

Manter logs locais evita tráfego de rede para escrita de alto volume. `URL` e `AuxURL` iguais retornam `ErrSameURL` (ambos têm `_migrations`).

| Campo | Obrigatório | Descrição |
|---|---|---|
| `URL` | sim | `libsql://`, `https://`, `http://`, `wss://`, `ws://` |
| `AuthToken` | Turso: sim | JWT |
| `AuxURL` | não | banco remoto para logs |
| `AuxAuthToken` | não | padrão = `AuthToken` |
| `LocalAux` | não | provedor para o auxiliar local; padrão = `sqlite` |

## Via ambiente

```bash
TURBINE_DB_PROVIDER=libsql
TURBINE_DB_DSN=libsql://minha-db-org.turso.io
TURBINE_DB_AUTH_TOKEN=eyJ...
TURBINE_DB_OPTIONS="aux_url=libsql://logs-org.turso.io"   # opcional
```

## Desenvolvimento local

```bash
docker run --rm -p 8080:8080 ghcr.io/tursodatabase/libsql-server:latest
TURBINEDB_LIBSQL_URL=http://127.0.0.1:8080 go test -run Integration -v ./...
```

## Limitações

- Cada escrita do Turbine é um round-trip de rede: meça a latência por passo antes de produção.
- `PRAGMA wal_checkpoint`/`optimize` periódicos do PocketBase podem falhar no remoto; o erro é só logado.
- Réplicas embarcadas (embedded replicas) exigem o driver cgo `go-libsql`; não suportadas por este provedor.
