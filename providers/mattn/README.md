# providers/mattn

Arquivos SQLite locais via [`mattn/go-sqlite3`](https://github.com/mattn/go-sqlite3) (cgo). Módulo separado.

**Requer** `CGO_ENABLED=1` e toolchain C.

Quando usar: extensões SQLite carregáveis, build de sistema do SQLite, forks compatíveis (ex.: SQLCipher via `replace`), ou benchmark comparativo contra o driver puro Go.

```bash
go get github.com/turbine-adapter/turbinedb/providers/mattn
```

```go
p := mattn.New(mattn.Config{
    Params: url.Values{"_busy_timeout": {"5000"}},
})
s, err := turbinedb.NewStandalone(turbinedb.Config{Provider: p})
```

`Params` é mesclado sobre `DefaultParams()` — equivalentes aos pragmas do PocketBase mais `_txlock=immediate`. Referência: [connection string do go-sqlite3](https://github.com/mattn/go-sqlite3#connection-string).

Via env (cada opção vira parâmetro DSN):

```bash
TURBINE_DB_PROVIDER=mattn
TURBINE_DB_OPTIONS="_busy_timeout=5000,_cache_size=-64000"
```

O binário continua linkando o driver modernc: o núcleo `turbinedb` usa o provedor `sqlite` como padrão. Os dois drivers coexistem (`sqlite` e `sqlite3` são nomes distintos).
