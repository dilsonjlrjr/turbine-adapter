// Package sqlite provides the default provider: local SQLite files through the
// pure-Go modernc.org/sqlite driver that PocketBase already ships with.
//
// It behaves like PocketBase's own DefaultDBConnect but lets callers tune the
// connection pragmas. Importing this package registers it as "sqlite".
package sqlite

import (
	"strings"

	"github.com/pocketbase/dbx"
	// Registers the "sqlite" database/sql driver (pure Go, no cgo).
	_ "modernc.org/sqlite"

	"github.com/turbine-adapter/turbinedb/provider"
)

// Name is the registry name of this provider.
const Name = "sqlite"

// driverName is the database/sql driver registered by modernc.org/sqlite.
const driverName = "sqlite"

// optionPragmas is the Settings option holding a ";"-separated pragma list.
const optionPragmas = "pragmas"

// DefaultPragmas returns the pragmas PocketBase applies by default.
// busy_timeout must stay first so the connection blocks on busy before WAL is enabled.
func DefaultPragmas() []string {
	return []string{
		"busy_timeout(10000)",
		"journal_mode(WAL)",
		"journal_size_limit(200000000)",
		"synchronous(NORMAL)",
		"foreign_keys(ON)",
		"temp_store(MEMORY)",
		"cache_size(-32000)",
	}
}

// Config configures the SQLite provider.
type Config struct {
	// Pragmas applied to every connection, e.g. "busy_timeout(5000)".
	// Nil means DefaultPragmas; an empty non-nil slice applies none.
	Pragmas []string
}

// Provider opens local SQLite files.
type Provider struct {
	dsnSuffix string
}

// New returns a SQLite provider.
func New(cfg Config) *Provider {
	pragmas := cfg.Pragmas
	if pragmas == nil {
		pragmas = DefaultPragmas()
	}
	return &Provider{dsnSuffix: buildDSNSuffix(pragmas)}
}

// Name implements provider.Provider.
func (*Provider) Name() string { return Name }

// Open implements provider.Provider. It opens the file at target.Path.
func (p *Provider) Open(target provider.Target) (*dbx.DB, error) {
	return dbx.Open(driverName, target.Path+p.dsnSuffix)
}

func buildDSNSuffix(pragmas []string) string {
	if len(pragmas) == 0 {
		return ""
	}
	var b strings.Builder
	for i, pragma := range pragmas {
		if i == 0 {
			b.WriteByte('?')
		} else {
			b.WriteByte('&')
		}
		b.WriteString("_pragma=")
		b.WriteString(pragma)
	}
	return b.String()
}

// FromSettings builds a provider from registry settings.
// Supported option: "pragmas" — ";"-separated list replacing the defaults.
// DSN is ignored: PocketBase decides the file paths from its data directory.
func FromSettings(settings provider.Settings) (provider.Provider, error) {
	cfg := Config{}
	if raw := settings.Option(optionPragmas, ""); raw != "" {
		cfg.Pragmas = splitList(raw)
	}
	return New(cfg), nil
}

func splitList(raw string) []string {
	parts := strings.Split(raw, ";")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

//nolint:gochecknoinits // registry self-registration, same pattern as database/sql drivers.
func init() {
	provider.Register(Name, FromSettings)
}
