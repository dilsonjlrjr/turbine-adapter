package turbinedb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/turbine-adapter/turbinedb/provider"
	"github.com/turbine-adapter/turbinedb/sqlite"
)

// Environment variables read by ConfigFromEnv.
const (
	EnvProvider        = "TURBINE_DB_PROVIDER"       // registry name; default "sqlite"
	EnvDSN             = "TURBINE_DB_DSN"            // provider-specific DSN / URL
	EnvAuthToken       = "TURBINE_DB_AUTH_TOKEN"     //nolint:gosec // env var name for an optional credential, not a secret
	EnvOptions         = "TURBINE_DB_OPTIONS"        // "key=value,key=value"
	EnvAuxProvider     = "TURBINE_DB_AUX_PROVIDER"   // optional: provider for the auxiliary DB
	EnvAuxDSN          = "TURBINE_DB_AUX_DSN"        // DSN for the auxiliary provider
	EnvAuxAuthToken    = "TURBINE_DB_AUX_AUTH_TOKEN" //nolint:gosec // env var name for the aux credential, not a secret
	EnvAuxOptions      = "TURBINE_DB_AUX_OPTIONS"    // options for the auxiliary provider
	EnvDataDir         = "TURBINE_DATA_DIR"          // PocketBase data dir; default "pb_data"
	EnvDev             = "TURBINE_DEV"               // bool
	EnvQueryTimeout    = "TURBINE_DB_QUERY_TIMEOUT"  // Go duration, e.g. "30s"
	EnvExecutorID      = "TURBINE_EXECUTOR_ID"       // turbine.Config.ExecutorID
	EnvAppVersion      = "TURBINE_APP_VERSION"       // turbine.Config.ApplicationVersion
	EnvGCRetention     = "TURBINE_GC_RETENTION"      // Go duration; negative disables GC
	EnvShutdownTimeout = "TURBINE_SHUTDOWN_TIMEOUT"  // Go duration
	EnvSysDB           = "TURBINE_SYSDB"             // system database registry name; empty = built-in (PocketBase SQLite)
	EnvSysDBDSN        = "TURBINE_SYSDB_DSN"         // system database DSN / URL
	EnvSysDBOptions    = "TURBINE_SYSDB_OPTIONS"     // "key=value,key=value"
)

// LookupFunc matches os.LookupEnv; injectable for tests and custom sources.
type LookupFunc func(key string) (string, bool)

// ConfigFromEnv builds a Config from TURBINE_* environment variables.
// Providers other than "sqlite" must be linked in with a blank import, e.g.
//
//	import _ "github.com/turbine-adapter/turbinedb/providers/libsql"
//
// When TURBINE_SYSDB is set, the named system database is opened (it may
// connect and migrate immediately) and stored in Config.Turbine.SystemDatabase;
// it must be linked in with a blank import too, e.g.
//
//	import _ "github.com/turbine-adapter/turbinedb/providers/postgres"
func ConfigFromEnv() (Config, error) {
	return ConfigFromLookup(os.LookupEnv)
}

// ConfigFromLookup is ConfigFromEnv with a custom variable source.
func ConfigFromLookup(lookup LookupFunc) (Config, error) {
	get := func(key string) string {
		v, _ := lookup(key)
		return strings.TrimSpace(v)
	}

	var (
		cfg  Config
		errs []error
	)

	p, err := providerFromEnv(get(EnvProvider), get(EnvDSN), get(EnvAuthToken), get(EnvOptions))
	errs = append(errs, err)
	cfg.Provider = p

	if name := get(EnvAuxProvider); name != "" {
		aux, auxErr := providerFromEnv(name, get(EnvAuxDSN), get(EnvAuxAuthToken), get(EnvAuxOptions))
		errs = append(errs, auxErr)
		cfg.AuxProvider = aux
	}

	cfg.DataDir = get(EnvDataDir)
	cfg.Turbine.ExecutorID = get(EnvExecutorID)
	cfg.Turbine.ApplicationVersion = get(EnvAppVersion)

	cfg.Dev, err = parseBool(EnvDev, get(EnvDev))
	errs = append(errs, err)
	cfg.QueryTimeout, err = parseDuration(EnvQueryTimeout, get(EnvQueryTimeout))
	errs = append(errs, err)
	cfg.Turbine.GCRetention, err = parseDuration(EnvGCRetention, get(EnvGCRetention))
	errs = append(errs, err)
	cfg.Turbine.ShutdownTimeout, err = parseDuration(EnvShutdownTimeout, get(EnvShutdownTimeout))
	errs = append(errs, err)

	if name := get(EnvSysDB); name != "" {
		sysOpts, optErr := parseOptions(get(EnvSysDBOptions))
		errs = append(errs, optErr)
		if optErr == nil {
			sysDB, sysErr := OpenSystemDatabase(context.Background(), name,
				Settings{DSN: get(EnvSysDBDSN), Options: sysOpts})
			errs = append(errs, sysErr)
			cfg.Turbine.SystemDatabase = sysDB
		}
	}

	if err := errors.Join(errs...); err != nil {
		return Config{}, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	return cfg, cfg.validate()
}

func providerFromEnv(name, dsn, token, rawOptions string) (Provider, error) {
	if name == "" {
		name = sqlite.Name
	}
	opts, err := parseOptions(rawOptions)
	if err != nil {
		return nil, err
	}
	return provider.Open(name, provider.Settings{DSN: dsn, AuthToken: token, Options: opts})
}

// parseOptions parses "key=value,key=value". Values may contain "=".
func parseOptions(raw string) (map[string]string, error) {
	opts := map[string]string{}
	if raw == "" {
		return opts, nil
	}
	for pair := range strings.SplitSeq(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		key, value, ok := strings.Cut(pair, "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" {
			return nil, fmt.Errorf("invalid option %q: expected key=value", pair)
		}
		opts[key] = strings.TrimSpace(value)
	}
	return opts, nil
}

func parseBool(key, raw string) (bool, error) {
	if raw == "" {
		return false, nil
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s: %w", key, err)
	}
	return v, nil
}

func parseDuration(key, raw string) (time.Duration, error) {
	if raw == "" {
		return 0, nil
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return v, nil
}
