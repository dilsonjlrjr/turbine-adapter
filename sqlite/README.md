# sqlite (provedor padrão)

Arquivos SQLite locais via `modernc.org/sqlite` (puro Go, sem cgo) — o mesmo driver que o PocketBase usa. Diferença para o padrão do PocketBase: **pragmas configuráveis**.

Registrado como `sqlite`. Usado automaticamente quando `Config.Provider` é nil.

```go
p := sqlite.New(sqlite.Config{
    Pragmas: append(sqlite.DefaultPragmas(), "mmap_size(268435456)"),
})
```

| `Config.Pragmas` | Efeito |
|---|---|
| `nil` | `DefaultPragmas()` (iguais aos do PocketBase) |
| `[]string{}` | nenhum pragma |
| lista | substitui os padrões — inclua `busy_timeout` primeiro |

Pragmas padrão: `busy_timeout(10000)`, `journal_mode(WAL)`, `journal_size_limit(200000000)`, `synchronous(NORMAL)`, `foreign_keys(ON)`, `temp_store(MEMORY)`, `cache_size(-32000)`.

Via env:

```bash
TURBINE_DB_PROVIDER=sqlite
TURBINE_DB_OPTIONS="pragmas=busy_timeout(5000);journal_mode(WAL);foreign_keys(ON)"
```

Caminho dos arquivos vem de `Config.DataDir`; `DSN` é ignorado.
