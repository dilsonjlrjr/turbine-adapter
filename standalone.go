package turbinedb

import (
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/YakirOren/turbine"
	"github.com/pocketbase/pocketbase/core"
)

// Standalone is a Turbine runtime that owns an embedded, non-HTTP PocketBase
// app backed by the configured providers. It is the provider-aware
// counterpart of turbine.NewStandalone.
//
//	s, err := turbinedb.NewStandalone(turbinedb.Config{Provider: p})
//	if err != nil { ... }
//	defer s.Shutdown()
//	turbine.Register(s.Runtime, MyWorkflow)
//	if err := s.Launch(); err != nil { ... }
type Standalone struct {
	// App is the embedded PocketBase app. Valid for queries after Launch.
	App *core.BaseApp
	// Runtime is the Turbine runtime. Register workflows on it before Launch.
	Runtime *turbine.Runtime

	shutdownOnce sync.Once
}

// NewStandalone builds (but does not launch) a standalone runtime.
// Workflow and step logs go to stdout unless Config.Turbine.Logger is set.
func NewStandalone(cfg Config) (*Standalone, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg = cfg.withDefaults()
	if cfg.Turbine.Logger == nil {
		cfg.Turbine.Logger = slog.New(slog.NewTextHandler(os.Stdout, nil))
	}

	app := core.NewBaseApp(cfg.baseAppConfig(cfg.Turbine.Logger))
	return &Standalone{
		App:     app,
		Runtime: turbine.SetupStandalone(app, cfg.Turbine),
	}, nil
}

// Launch bootstraps the app (opening every database through the providers),
// applies migrations and starts the Turbine runtime.
func (s *Standalone) Launch() error {
	if err := s.App.Bootstrap(); err != nil {
		return fmt.Errorf("turbinedb: bootstrap failed: %w", err)
	}
	if err := s.Runtime.Launch(); err != nil {
		return fmt.Errorf("turbinedb: launch failed: %w", err)
	}
	return nil
}

// Shutdown drains the runtime and closes every database connection.
// Safe to call more than once and before Launch.
func (s *Standalone) Shutdown() {
	s.shutdownOnce.Do(func() {
		s.Runtime.Shutdown()
		_ = s.App.ResetBootstrapState()
	})
}
