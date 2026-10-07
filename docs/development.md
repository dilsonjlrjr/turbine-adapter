# Desenvolvimento

## Layout de módulos

O repositório tem **cinco módulos Go**, amarrados por `go.work` para desenvolvimento local:

| Módulo | Diretório | Por que separado |
|---|---|---|
| `github.com/turbine-adapter/turbinedb` | `.` | núcleo + provedor sqlite (sem deps extras) |
| `…/providers/mattn` | `providers/mattn` | exige cgo |
| `…/providers/libsql` | `providers/libsql` | puxa o cliente libSQL |
| `…/providers/postgres` | `providers/postgres` | puxa `pgx`; implementa `turbine.SystemDatabase` (exige o fork do Turbine) |
| `…/examples` | `examples` | binários de exemplo, não devem poluir o `go.mod` do núcleo |

Os submódulos têm `replace github.com/turbine-adapter/turbinedb => ../..` para funcionar antes da primeira tag. **Remova os `replace` ao publicar** e passe a exigir a versão taggeada.

Tags de release seguem o caminho do módulo: `v0.1.0`, `providers/libsql/v0.1.0`, `providers/mattn/v0.1.0`, `providers/postgres/v0.1.0`.

Todos os `go.mod` têm `replace github.com/YakirOren/turbine => github.com/dilsonjlrjr/turbine <pseudo-versão>` (fork `feature/pluggable-sysdb`). Ao resolver módulos use `GONOSUMDB=github.com/dilsonjlrjr GOPROXY=direct,https://proxy.golang.org`.

## Comandos

```bash
make build        # compila todos os módulos
make vet          # go vet em todos
make lint         # golangci-lint (config em .golangci.yml)
make fmt          # gofumpt + goimports
make test         # todos os testes, -race, CGO_ENABLED=1
make test-short   # só o núcleo, sem cgo
make test-postgres # só providers/postgres (DSN externo ou Postgres embutido)
make tidy         # go mod tidy em todos
make check        # build + vet + lint + test (o que a CI roda)
```

Teste único:

```bash
go test -run TestStandaloneRoutesRolesToProviders -v .
cd providers/mattn && go test -run TestRunsTurbineWorkflow -v ./...
```

## Integração libSQL

```bash
docker run --rm -p 8080:8080 ghcr.io/tursodatabase/libsql-server:latest
TURBINEDB_LIBSQL_URL=http://127.0.0.1:8080 make test-libsql
```

Sem `TURBINEDB_LIBSQL_URL` o teste de integração é pulado (`t.Skip`).

## Testes do Postgres

`providers/postgres` roda a suíte de conformidade do Turbine (`sysdbtest.RunSuite`), um teste multi-instância (LISTEN/NOTIFY, sem `double-claim`, `GlobalConcurrency`), um ponta a ponta com `NewStandalone` e o caminho por variáveis de ambiente.

- `TURBINEDB_POSTGRES_DSN` definida: usa esse servidor (cada teste cria e remove um schema aleatório).
- Sem ela: sobe `fergusstrange/embedded-postgres` uma vez no `TestMain` (porta livre, diretório temporário apagado no fim). Na primeira execução baixa os binários do Postgres; precisa de rede e não roda como root.

```bash
make test-postgres
TURBINEDB_POSTGRES_DSN='postgres://postgres:pg@127.0.0.1:5432/postgres?sslmode=disable' make test-postgres
```

Na CI, o job `postgres-integration` usa um serviço `postgres:17`.

## Convenções

- Testes em pacote `_test` externo (linter `testpackage`).
- Todo provedor novo precisa de um teste que roda um workflow Turbine real ponta a ponta (ver `runGreet` em `standalone_test.go`).
- `init()` só para auto-registro de provedor, com `//nolint:gochecknoinits` justificado.
- Nenhuma dependência de driver no módulo raiz além do que o PocketBase já traz.
