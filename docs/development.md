# Desenvolvimento

## Layout de módulos

O repositório tem **quatro módulos Go**, amarrados por `go.work` para desenvolvimento local:

| Módulo | Diretório | Por que separado |
|---|---|---|
| `github.com/turbine-adapter/turbinedb` | `.` | núcleo + provedor sqlite (sem deps extras) |
| `…/providers/mattn` | `providers/mattn` | exige cgo |
| `…/providers/libsql` | `providers/libsql` | puxa o cliente libSQL |
| `…/examples` | `examples` | binários de exemplo, não devem poluir o `go.mod` do núcleo |

Os submódulos têm `replace github.com/turbine-adapter/turbinedb => ../..` para funcionar antes da primeira tag. **Remova os `replace` ao publicar** e passe a exigir a versão taggeada.

Tags de release seguem o caminho do módulo: `v0.1.0`, `providers/libsql/v0.1.0`, `providers/mattn/v0.1.0`.

## Comandos

```bash
make build        # compila todos os módulos
make vet          # go vet em todos
make lint         # golangci-lint (config em .golangci.yml)
make fmt          # gofumpt + goimports
make test         # todos os testes, -race, CGO_ENABLED=1
make test-short   # só o núcleo, sem cgo
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

## Convenções

- Testes em pacote `_test` externo (linter `testpackage`).
- Todo provedor novo precisa de um teste que roda um workflow Turbine real ponta a ponta (ver `runGreet` em `standalone_test.go`).
- `init()` só para auto-registro de provedor, com `//nolint:gochecknoinits` justificado.
- Nenhuma dependência de driver no módulo raiz além do que o PocketBase já traz.
