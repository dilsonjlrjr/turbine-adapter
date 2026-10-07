package turbinedb_test

import (
	"errors"
	"testing"
	"time"

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
