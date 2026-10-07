package turbinedb_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/YakirOren/turbine"

	"github.com/turbine-adapter/turbinedb"
	"github.com/turbine-adapter/turbinedb/provider"
	"github.com/turbine-adapter/turbinedb/sqlite"
)

func lookupFrom(env map[string]string) turbinedb.LookupFunc {
	return func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	}
}

func TestConfigFromLookupDefaults(t *testing.T) {
	cfg, err := turbinedb.ConfigFromLookup(lookupFrom(nil))
	if err != nil {
		t.Fatalf("ConfigFromLookup: %v", err)
	}
	if cfg.Provider == nil || cfg.Provider.Name() != sqlite.Name {
		t.Fatalf("default provider = %v, want sqlite", cfg.Provider)
	}
	if cfg.AuxProvider != nil {
		t.Fatal("AuxProvider must stay nil when not configured")
	}
}

func TestConfigFromLookupFull(t *testing.T) {
	cfg, err := turbinedb.ConfigFromLookup(lookupFrom(map[string]string{
		turbinedb.EnvProvider:        "sqlite",
		turbinedb.EnvOptions:         "pragmas=busy_timeout(5000)",
		turbinedb.EnvAuxProvider:     "sqlite",
		turbinedb.EnvDataDir:         "/tmp/pb",
		turbinedb.EnvDev:             "true",
		turbinedb.EnvQueryTimeout:    "5s",
		turbinedb.EnvExecutorID:      "worker-1",
		turbinedb.EnvAppVersion:      "v9",
		turbinedb.EnvGCRetention:     "-1s",
		turbinedb.EnvShutdownTimeout: "2s",
	}))
	if err != nil {
		t.Fatalf("ConfigFromLookup: %v", err)
	}
	if cfg.AuxProvider == nil || cfg.DataDir != "/tmp/pb" || !cfg.Dev || cfg.QueryTimeout != 5*time.Second {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	tc := cfg.Turbine
	if tc.ExecutorID != "worker-1" || tc.ApplicationVersion != "v9" ||
		tc.GCRetention != -time.Second || tc.ShutdownTimeout != 2*time.Second {
		t.Fatalf("unexpected turbine config: %+v", tc)
	}
}

func TestConfigFromLookupErrors(t *testing.T) {
	cases := map[string]map[string]string{
		"unknown provider": {turbinedb.EnvProvider: "nope"},
		"bad bool":         {turbinedb.EnvDev: "maybe"},
		"bad duration":     {turbinedb.EnvQueryTimeout: "soon"},
		"bad options":      {turbinedb.EnvOptions: "novalue"},
		"negative timeout": {turbinedb.EnvQueryTimeout: "-1s"},
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := turbinedb.ConfigFromLookup(lookupFrom(env))
			if !errors.Is(err, turbinedb.ErrInvalidConfig) && !errors.Is(err, provider.ErrUnknownProvider) {
				t.Fatalf("expected config error, got %v", err)
			}
		})
	}
}

// fakeSysDB is a placeholder SystemDatabase; the embedded nil interface is never called.
type fakeSysDB struct {
	turbine.SystemDatabase
	settings provider.Settings
}

func registerFakeSysDB(t *testing.T, name string) *fakeSysDB {
	t.Helper()
	fake := &fakeSysDB{}
	turbinedb.RegisterSystemDatabase(name, func(_ context.Context, s turbinedb.Settings) (turbine.SystemDatabase, error) {
		if s.DSN == "boom" {
			return nil, errors.New("connection refused")
		}
		fake.settings = s
		return fake, nil
	})
	return fake
}

func TestConfigFromLookupSystemDatabase(t *testing.T) {
	fake := registerFakeSysDB(t, "fake-env-ok")

	cfg, err := turbinedb.ConfigFromLookup(lookupFrom(map[string]string{
		turbinedb.EnvSysDB:        "fake-env-ok",
		turbinedb.EnvSysDBDSN:     "postgres://u:p@h/db",
		turbinedb.EnvSysDBOptions: "schema=x, max_conns=4",
	}))
	if err != nil {
		t.Fatalf("ConfigFromLookup: %v", err)
	}
	if cfg.Turbine.SystemDatabase != turbine.SystemDatabase(fake) {
		t.Fatalf("SystemDatabase = %v, want the fake", cfg.Turbine.SystemDatabase)
	}
	if fake.settings.DSN != "postgres://u:p@h/db" ||
		fake.settings.Options["schema"] != "x" || fake.settings.Options["max_conns"] != "4" {
		t.Fatalf("settings = %+v", fake.settings)
	}

	cfg, err = turbinedb.ConfigFromLookup(lookupFrom(nil))
	if err != nil || cfg.Turbine.SystemDatabase != nil {
		t.Fatalf("empty TURBINE_SYSDB must keep the built-in: %v / %v", cfg.Turbine.SystemDatabase, err)
	}
}

func TestConfigFromLookupSystemDatabaseErrors(t *testing.T) {
	registerFakeSysDB(t, "fake-env-err")
	cases := map[string]map[string]string{
		"unknown":       {turbinedb.EnvSysDB: "does-not-exist"},
		"factory error": {turbinedb.EnvSysDB: "fake-env-err", turbinedb.EnvSysDBDSN: "boom"},
		"bad options":   {turbinedb.EnvSysDB: "fake-env-err", turbinedb.EnvSysDBOptions: "novalue"},
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := turbinedb.ConfigFromLookup(lookupFrom(env))
			if !errors.Is(err, turbinedb.ErrInvalidConfig) {
				t.Fatalf("expected ErrInvalidConfig, got %v", err)
			}
		})
	}
	_, err := turbinedb.OpenSystemDatabase(context.Background(), "does-not-exist", turbinedb.Settings{})
	if !errors.Is(err, turbinedb.ErrUnknownSystemDatabase) {
		t.Fatalf("expected ErrUnknownSystemDatabase, got %v", err)
	}
}

func TestRegisterSystemDatabasePanics(t *testing.T) {
	registerFakeSysDB(t, "fake-dup")
	for name, fn := range map[string]func(){
		"empty name": func() {
			turbinedb.RegisterSystemDatabase("", func(context.Context, turbinedb.Settings) (turbine.SystemDatabase, error) { return nil, nil })
		},
		"nil":       func() { turbinedb.RegisterSystemDatabase("fake-nil", nil) },
		"duplicate": func() { registerFakeSysDB(t, "fake-dup") },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			fn()
		})
	}
	if !slices.Contains(turbinedb.RegisteredSystemDatabases(), "fake-dup") {
		t.Fatal("fake-dup missing from RegisteredSystemDatabases")
	}
}
