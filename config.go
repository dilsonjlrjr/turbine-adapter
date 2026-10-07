package turbinedb

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/YakirOren/turbine"
	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"

	"github.com/turbine-adapter/turbinedb/provider"
	"github.com/turbine-adapter/turbinedb/sqlite"
)

// DefaultDataDir is used when Config.DataDir is empty.
const DefaultDataDir = "pb_data"

// ErrInvalidConfig wraps every configuration validation error.
var ErrInvalidConfig = errors.New("turbinedb: invalid config")

// Re-exported so most callers only import this package.
type (
	// Provider opens SQLite-dialect connections for PocketBase. See provider.Provider.
	Provider = provider.Provider
	// Target describes the database being opened. See provider.Target.
	Target = provider.Target
	// Settings is the generic provider configuration. See provider.Settings.
	Settings = provider.Settings
)

// Config is everything needed to build a Turbine runtime on a chosen database provider.
// The zero value is valid: local SQLite files under ./pb_data.
type Config struct {
	// Provider opens the data database (and the auxiliary one when AuxProvider is nil).
	// Nil means the local SQLite provider with PocketBase's default pragmas.
	Provider Provider
	// AuxProvider optionally opens the auxiliary (request logs) database with a different provider.
	AuxProvider Provider

	// DataDir is PocketBase's data directory. Local providers store their files here;
	// uploaded files and backups always live here. Defaults to DefaultDataDir.
	DataDir string
	// Dev enables PocketBase dev mode (SQL statement logging).
	Dev bool
	// EncryptionEnv names the env var holding the PocketBase settings encryption key.
	EncryptionEnv string

	// QueryTimeout bounds every PocketBase query. Zero uses PocketBase's default.
	QueryTimeout time.Duration
	// Pool sizes. Zero values use PocketBase's defaults.
	DataMaxOpenConns int
	DataMaxIdleConns int
	AuxMaxOpenConns  int
	AuxMaxIdleConns  int

	// Turbine is passed through unchanged to the Turbine runtime.
	Turbine turbine.Config
}

func (c Config) withDefaults() Config {
	if c.Provider == nil {
		c.Provider = sqlite.New(sqlite.Config{})
	}
	if c.AuxProvider == nil {
		c.AuxProvider = c.Provider
	}
	if c.DataDir == "" {
		c.DataDir = DefaultDataDir
	}
	return c
}

func (c Config) validate() error {
	var errs []error
	if c.QueryTimeout < 0 {
		errs = append(errs, errors.New("QueryTimeout must not be negative"))
	}
	for name, v := range map[string]int{
		"DataMaxOpenConns": c.DataMaxOpenConns,
		"DataMaxIdleConns": c.DataMaxIdleConns,
		"AuxMaxOpenConns":  c.AuxMaxOpenConns,
		"AuxMaxIdleConns":  c.AuxMaxIdleConns,
	} {
		if v < 0 {
			errs = append(errs, fmt.Errorf("%s must not be negative", name))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%w: %w", ErrInvalidConfig, errors.Join(errs...))
	}
	return nil
}

// dbConnect adapts the configured providers to PocketBase's DBConnect hook.
func (c Config) dbConnect(logger *slog.Logger) core.DBConnectFunc {
	return func(dbPath string) (*dbx.DB, error) {
		target := provider.TargetFromPath(dbPath)
		p := c.Provider
		if target.Role == provider.RoleAuxiliary {
			p = c.AuxProvider
		}

		db, err := p.Open(target)
		if err != nil {
			return nil, fmt.Errorf("turbinedb: provider %q failed to open %s database: %w", p.Name(), target.Role, err)
		}
		if logger != nil {
			logger.Debug("turbinedb: database opened", "provider", p.Name(), "role", string(target.Role))
		}
		return db, nil
	}
}

func (c Config) baseAppConfig(logger *slog.Logger) core.BaseAppConfig {
	return core.BaseAppConfig{
		DBConnect:        c.dbConnect(logger),
		DataDir:          c.DataDir,
		EncryptionEnv:    c.EncryptionEnv,
		QueryTimeout:     c.QueryTimeout,
		DataMaxOpenConns: c.DataMaxOpenConns,
		DataMaxIdleConns: c.DataMaxIdleConns,
		AuxMaxOpenConns:  c.AuxMaxOpenConns,
		AuxMaxIdleConns:  c.AuxMaxIdleConns,
		IsDev:            c.Dev,
	}
}
