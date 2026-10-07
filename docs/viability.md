# Análise de viabilidade

Baseada no Turbine `v0.3.0` (commit `ba535c5`) e PocketBase `v0.36.8`.

## Como o Turbine persiste dados

- Toda a persistência passa por `core.App` do PocketBase.
- Existe uma interface interna `systemDatabase` (`types.go`) com ~30 métodos, implementada apenas por `sqliteSysDB` (`sysdb_sqlite.go`, ~1.400 linhas).
- A interface é **não exportada** e `NewRuntime` instancia `newSQLiteSysDB` diretamente — não há injeção.
- Além do `sysDB`, 15 arquivos usam `core.App` diretamente: coleções/migrações (`collections.go`), hooks de registro (`plugin.go`, `webhooks.go`), cron (`schedule_manager.go`), dashboard, produtos, notificações.
- SQL específico: `ON CONFLICT … DO UPDATE` e `RETURNING` em `sysdb_sqlite.go`; o PocketBase em si usa `PRAGMA`, funções JSON do SQLite e tabelas `_collections`/`_migrations` no dialeto SQLite.

## Caminhos avaliados

| Caminho | Viável sem fork? | Observação |
|---|---|---|
| Implementar `systemDatabase` para outro banco | **Não** | Interface privada, construção fixa em `NewRuntime` |
| Trocar o banco do PocketBase por Postgres/MySQL | **Não** | PocketBase só suporta dialeto SQLite (migrações, filtros, schema) |
| Hook `DBConnect` do PocketBase com driver compatível com SQLite | **Sim** | API pública, e `turbine.Setup`/`SetupStandalone` aceitam app externo |

## Decisão

O adaptador usa o hook `DBConnect`. Isso cobre qualquer backend que fale dialeto SQLite:

- SQLite local com driver/pragma diferentes (modernc, mattn/cgo, builds customizados);
- libSQL remoto (Turso, `sqld` self-hosted);
- candidatos futuros: `ncruces/go-sqlite3` (WASM), SQLCipher, rqlite (dialeto SQLite, semântica distribuída — exige validação).

## Riscos conhecidos

- **Latência remota**: o Turbine faz muitas escritas pequenas por passo de workflow; libSQL remoto adiciona RTT a cada uma. Adequado para volume moderado; medir antes de produção.
- **PRAGMAs em remoto**: o PocketBase roda `PRAGMA wal_checkpoint`/`optimize` periodicamente; falhas são só logadas.
- **Transações**: o PocketBase usa um pool de escrita de conexão única; o provedor remoto precisa suportar transações interativas (libSQL via Hrana suporta).
- **Upgrade do Turbine**: o adaptador depende só de `Setup`, `SetupStandalone` e `Config` públicos. Mudança nessas assinaturas quebra o adaptador; o resto do Turbine pode evoluir livremente.

## Caminho para Postgres (fora de escopo)

Exigiria, no upstream: exportar `systemDatabase` + opção de injeção em `Config`, e uma implementação Postgres. Mesmo assim o restante (coleções, dashboard, hooks) continuaria preso ao PocketBase/SQLite. Não recomendado como extensão desacoplada.
