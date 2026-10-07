// Package libsql provides remote libSQL databases (Turso, self-hosted sqld)
// through the pure-Go client github.com/tursodatabase/libsql-client-go.
//
// The data database (collections + every Turbine pt_* table) goes to URL.
// The auxiliary database (PocketBase request logs) goes to AuxURL when set;
// otherwise it stays in a local SQLite file under the PocketBase data dir,
// which avoids shipping high-volume log writes over the network.
//
// Data and auxiliary databases MUST be different: both own a _migrations table.
//
// Importing this package registers it as "libsql".
package libsql

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/pocketbase/dbx"
	"github.com/tursodatabase/libsql-client-go/libsql"

	"github.com/turbine-adapter/turbinedb/provider"
	"github.com/turbine-adapter/turbinedb/sqlite"
)

// Name is the registry name of this provider.
const Name = "libsql"

// Registry option keys understood by FromSettings.
const (
	OptionAuxURL       = "aux_url"
	OptionAuxAuthToken = "aux_auth_token" //nolint:gosec // option key, not a secret
)

// dbxBuilder selects dbx's SQLite query builder for the libsql driver.
const dbxBuilder = "sqlite"

// ErrMissingURL is returned when Config.URL is empty.
var ErrMissingURL = errors.New("libsql: URL is required")

// ErrSameURL is returned when the data and auxiliary databases point to the same URL.
var ErrSameURL = errors.New("libsql: URL and AuxURL must differ")

// Config configures the libSQL provider.
type Config struct {
	// URL of the data database: libsql://, https://, http://, wss:// or ws://.
	URL string
	// AuthToken is the JWT used for URL (and AuxURL when AuxAuthToken is empty).
	AuthToken string
	// AuxURL optionally places the auxiliary (logs) database remotely too.
	AuxURL string
	// AuxAuthToken overrides AuthToken for AuxURL.
	AuxAuthToken string
	// LocalAux opens the auxiliary database when AuxURL is empty.
	// Nil means the default local SQLite provider.
	LocalAux provider.Provider
}

// Provider opens libSQL connections.
type Provider struct {
	cfg Config
}

// New validates cfg and returns a libSQL provider.
func New(cfg Config) (*Provider, error) {
	if cfg.URL == "" {
		return nil, ErrMissingURL
	}
	if cfg.AuxURL != "" && cfg.AuxURL == cfg.URL {
		return nil, ErrSameURL
	}
	if cfg.AuxAuthToken == "" {
		cfg.AuxAuthToken = cfg.AuthToken
	}
	if cfg.AuxURL == "" && cfg.LocalAux == nil {
		cfg.LocalAux = sqlite.New(sqlite.Config{})
	}
	return &Provider{cfg: cfg}, nil
}

// Name implements provider.Provider.
func (*Provider) Name() string { return Name }

// Open implements provider.Provider.
func (p *Provider) Open(target provider.Target) (*dbx.DB, error) {
	url, token := p.cfg.URL, p.cfg.AuthToken
	if target.Role == provider.RoleAuxiliary {
		if p.cfg.AuxURL == "" {
			return p.cfg.LocalAux.Open(target)
		}
		url, token = p.cfg.AuxURL, p.cfg.AuxAuthToken
	}

	var opts []libsql.Option
	if token != "" {
		opts = append(opts, libsql.WithAuthToken(token))
	}
	connector, err := libsql.NewConnector(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("libsql: connector for %s database: %w", target.Role, err)
	}
	return dbx.NewFromDB(sql.OpenDB(connector), dbxBuilder), nil
}

// FromSettings builds a provider from registry settings:
// DSN → URL, AuthToken → AuthToken, options "aux_url" and "aux_auth_token".
func FromSettings(settings provider.Settings) (provider.Provider, error) {
	return New(Config{
		URL:          settings.DSN,
		AuthToken:    settings.AuthToken,
		AuxURL:       settings.Option(OptionAuxURL, ""),
		AuxAuthToken: settings.Option(OptionAuxAuthToken, ""),
	})
}

//nolint:gochecknoinits // registry self-registration, same pattern as database/sql drivers.
func init() {
	provider.Register(Name, FromSettings)
}
