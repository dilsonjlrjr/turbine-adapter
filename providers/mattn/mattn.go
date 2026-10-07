// Package mattn provides local SQLite files through the cgo driver
// github.com/mattn/go-sqlite3. Requires CGO_ENABLED=1 and a C toolchain.
//
// Useful when you need cgo-only SQLite features (custom extensions, a
// system SQLite build, SQLCipher-compatible forks via replace directives).
// Importing this package registers it as "mattn".
package mattn

import (
	"maps"
	"net/url"

	// Registers the "sqlite3" database/sql driver (cgo).
	_ "github.com/mattn/go-sqlite3"
	"github.com/pocketbase/dbx"

	"github.com/turbine-adapter/turbinedb/provider"
)

// Name is the registry name of this provider.
const Name = "mattn"

const driverName = "sqlite3"

// DefaultParams returns DSN parameters equivalent to PocketBase's default pragmas.
func DefaultParams() url.Values {
	return url.Values{
		"_busy_timeout": {"10000"},
		"_journal_mode": {"WAL"},
		"_synchronous":  {"NORMAL"},
		"_foreign_keys": {"on"},
		"_cache_size":   {"-32000"},
		"_txlock":       {"immediate"},
	}
}

// Config configures the mattn provider.
type Config struct {
	// Params are go-sqlite3 DSN parameters merged over DefaultParams
	// (see https://github.com/mattn/go-sqlite3#connection-string).
	Params url.Values
}

// Provider opens local SQLite files with mattn/go-sqlite3.
type Provider struct {
	query string
}

// New returns a mattn provider.
func New(cfg Config) *Provider {
	params := DefaultParams()
	maps.Copy(params, cfg.Params)
	return &Provider{query: params.Encode()}
}

// Name implements provider.Provider.
func (*Provider) Name() string { return Name }

// Open implements provider.Provider. It opens the file at target.Path.
func (p *Provider) Open(target provider.Target) (*dbx.DB, error) {
	return dbx.Open(driverName, "file:"+target.Path+"?"+p.query)
}

// FromSettings builds a provider from registry settings.
// Every option is passed as a go-sqlite3 DSN parameter, e.g. "_busy_timeout=5000".
// DSN is ignored: PocketBase decides the file paths from its data directory.
func FromSettings(settings provider.Settings) (provider.Provider, error) {
	params := url.Values{}
	for k, v := range settings.Options {
		params.Set(k, v)
	}
	return New(Config{Params: params}), nil
}

//nolint:gochecknoinits // registry self-registration, same pattern as database/sql drivers.
func init() {
	provider.Register(Name, FromSettings)
}
