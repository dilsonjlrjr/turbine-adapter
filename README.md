# turbinedb

Adaptador de provedores de banco de dados para o [Turbine](https://github.com/YakirOren/turbine) — motor de workflows duráveis em Go construído sobre PocketBase.

O Turbine grava tudo em SQLite local (`pb_data/data.db`). O `turbinedb` deixa você trocar **onde e como** esse banco é aberto — libSQL/Turso remoto, SQLite via cgo, SQLite com pragmas próprios — **sem fork** e sem alterar uma linha do Turbine.

```go
p, _ := libsql.New(libsql.Config{URL: "libsql://minha-db.turso.io", AuthToken: token})

s, _ := turbinedb.NewStandalone(turbinedb.Config{Provider: p})
defer s.Shutdown()

turbine.Register(s.Runtime, MeuWorkflow)
s.Launch()
```

## Por que funciona sem fork

O Turbine depende de `core.App` do PocketBase e aceita um app pronto em `turbine.Setup` / `turbine.SetupStandalone`. O PocketBase, por sua vez, expõe um único ponto de troca de armazenamento: o hook `DBConnect`. O adaptador constrói o app com esse hook apontando para um `Provider` e o entrega ao Turbine. Detalhes em [docs/architecture.md](docs/architecture.md).

**Restrição:** o provedor precisa falar **dialeto SQLite**. PostgreSQL/MySQL não são viáveis sem fork — ver [docs/viability.md](docs/viability.md).

## Provedores

| Provedor | Pacote / módulo | cgo | Uso |
|---|---|---|---|
| `sqlite` (padrão) | `github.com/turbine-adapter/turbinedb/sqlite` | não | Arquivo local, pragmas configuráveis |
| `mattn` | `github.com/turbine-adapter/turbinedb/providers/mattn` | **sim** | Arquivo local via `mattn/go-sqlite3` |
| `libsql` | `github.com/turbine-adapter/turbinedb/providers/libsql` | não | Turso / sqld remoto |

Cada provedor com dependência própria vive em **módulo Go separado**: você só baixa o driver que usa.

## Instalação

```bash
go get github.com/turbine-adapter/turbinedb
go get github.com/turbine-adapter/turbinedb/providers/libsql   # opcional
```

## Três formas de configurar

1. **Explícita** — instancie o provedor e passe em `Config.Provider`.
2. **Por nome** — `provider.Open("libsql", provider.Settings{DSN: url, AuthToken: tok})`.
3. **Por ambiente** — `turbinedb.ConfigFromEnv()` + import em branco do provedor:

```bash
TURBINE_DB_PROVIDER=libsql \
TURBINE_DB_DSN=libsql://minha-db.turso.io \
TURBINE_DB_AUTH_TOKEN=... \
./meu-servico
```

Referência completa de variáveis: [docs/configuration.md](docs/configuration.md).

## Modos de execução

| Turbine original | turbinedb | Quando usar |
|---|---|---|
| `turbine.NewStandalone(cfg)` | `turbinedb.NewStandalone(cfg)` | Workers, scripts, sem HTTP |
| `turbine.NewApp(cfg)` | `turbinedb.NewApp(cfg)` | Servidor PocketBase + dashboard |

`Config.Turbine` repassa `turbine.Config` sem alteração.

## Mapa da documentação

| Contexto | Arquivo |
|---|---|
| Arquitetura e fluxo de conexão | [docs/architecture.md](docs/architecture.md) |
| Análise de viabilidade | [docs/viability.md](docs/viability.md) |
| Configuração (struct + env) | [docs/configuration.md](docs/configuration.md) |
| Desenvolvimento, testes, lint | [docs/development.md](docs/development.md) |
| Contrato `Provider` e como escrever um novo | [provider/README.md](provider/README.md) |
| Provedor SQLite padrão | [sqlite/README.md](sqlite/README.md) |
| Provedor mattn (cgo) | [providers/mattn/README.md](providers/mattn/README.md) |
| Provedor libSQL / Turso | [providers/libsql/README.md](providers/libsql/README.md) |
| Exemplos executáveis | [examples/README.md](examples/README.md) |
