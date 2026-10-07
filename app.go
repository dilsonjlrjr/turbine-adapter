package turbinedb

import (
	"github.com/YakirOren/turbine"
	"github.com/pocketbase/pocketbase"
)

// NewApp is the provider-aware counterpart of turbine.NewApp.
//
// It builds a PocketBase app (HTTP server, admin UI, CLI) whose databases are
// opened by the configured providers and wires a Turbine runtime into its
// lifecycle. Register workflows on the returned runtime, then call app.Start().
//
// Config.DataDir is used as the default for PocketBase's --dir flag.
func NewApp(cfg Config) (*pocketbase.PocketBase, *turbine.Runtime, error) {
	if err := cfg.validate(); err != nil {
		return nil, nil, err
	}
	cfg = cfg.withDefaults()

	app := pocketbase.NewWithConfig(pocketbase.Config{
		DefaultDev:           cfg.Dev,
		DefaultDataDir:       cfg.DataDir,
		DefaultEncryptionEnv: cfg.EncryptionEnv,
		DefaultQueryTimeout:  cfg.QueryTimeout,
		DataMaxOpenConns:     cfg.DataMaxOpenConns,
		DataMaxIdleConns:     cfg.DataMaxIdleConns,
		AuxMaxOpenConns:      cfg.AuxMaxOpenConns,
		AuxMaxIdleConns:      cfg.AuxMaxIdleConns,
		DBConnect:            cfg.dbConnect(cfg.Turbine.Logger),
	})

	return app, turbine.Setup(app, cfg.Turbine), nil
}
