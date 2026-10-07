// Package turbinedb runs Turbine (github.com/YakirOren/turbine) on pluggable
// database providers without forking it.
//
// Turbine stores everything through PocketBase, and PocketBase exposes a
// single seam for storage: the DBConnect hook. This package builds the
// PocketBase app with that hook pointed at a Provider and then hands the app
// to Turbine's public Setup / SetupStandalone entry points.
//
// Any provider must speak the SQLite dialect (PocketBase and Turbine emit
// SQLite SQL: ON CONFLICT, RETURNING, PRAGMA, json functions). Bundled
// providers:
//
//   - sqlite (this module): local files, pure Go, tunable pragmas. Default.
//   - providers/mattn (separate module): local files via cgo mattn/go-sqlite3.
//   - providers/libsql (separate module): remote libSQL / Turso / sqld.
//
// Three ways to configure, from most explicit to most automatic:
//
//	turbinedb.NewStandalone(turbinedb.Config{Provider: libsql.New(libsql.Config{URL: u})})
//	turbinedb.NewStandalone(turbinedb.Config{Provider: must(provider.Open("libsql", settings))})
//	cfg, _ := turbinedb.ConfigFromEnv(); turbinedb.NewStandalone(cfg)
package turbinedb
