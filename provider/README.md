# provider

Contrato que todo provedor implementa e registro por nome.

```go
type Provider interface {
    Name() string
    Open(target Target) (*dbx.DB, error)
}

type Target struct {
    Role Role   // RoleData | RoleAuxiliary
    Path string // <DataDir>/data.db ou <DataDir>/auxiliary.db
}
```

## Regras do contrato

1. **Dialeto SQLite.** PocketBase e Turbine emitem SQL SQLite (`ON CONFLICT`, `RETURNING`, `PRAGMA`, funções JSON).
2. **Nova conexão a cada `Open`.** O PocketBase chama `Open` duas vezes por banco (pool concorrente + pool de escrita).
3. **Builder SQLite.** Se o driver não se chama `sqlite`/`sqlite3`, retorne `dbx.NewFromDB(sqlDB, "sqlite")`.
4. **Data ≠ auxiliar.** Os dois bancos têm `_migrations` próprios; nunca aponte ambos para o mesmo destino.
5. **Sem I/O no construtor** além de validação — a conexão acontece em `Open`, durante `Bootstrap`.

## Escrevendo um provedor

```go
package meuprov

type Provider struct{ dsn string }

func New(dsn string) *Provider { return &Provider{dsn: dsn} }

func (*Provider) Name() string { return "meuprov" }

func (p *Provider) Open(t provider.Target) (*dbx.DB, error) {
    db, err := sql.Open("meudriver", p.dsnFor(t.Role))
    if err != nil {
        return nil, err
    }
    return dbx.NewFromDB(db, "sqlite"), nil
}

func init() {
    provider.Register("meuprov", func(s provider.Settings) (provider.Provider, error) {
        return New(s.DSN), nil
    })
}
```

Coloque-o em módulo próprio sob `providers/` se trouxer dependência nova, e inclua um teste que executa um workflow Turbine real (ver `providers/mattn/mattn_test.go`).

## Registro

| Função | Uso |
|---|---|
| `Register(name, factory)` | no `init` do provedor; entra em pânico em nome vazio, factory nil ou duplicado |
| `Open(name, settings)` | instancia pelo nome; `ErrUnknownProvider` lista os registrados |
| `Registered()` | nomes registrados, ordenados |
