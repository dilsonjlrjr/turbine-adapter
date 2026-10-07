package turbinedb_test

import (
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/YakirOren/turbine"
	"github.com/pocketbase/dbx"

	"github.com/turbine-adapter/turbinedb"
	"github.com/turbine-adapter/turbinedb/provider"
	"github.com/turbine-adapter/turbinedb/sqlite"
)

// recordingProvider wraps the SQLite provider and records every Open call.
type recordingProvider struct {
	inner provider.Provider
	mu    sync.Mutex
	roles []provider.Role
}

func (*recordingProvider) Name() string { return "recording" }

func (r *recordingProvider) Open(target provider.Target) (*dbx.DB, error) {
	r.mu.Lock()
	r.roles = append(r.roles, target.Role)
	r.mu.Unlock()
	return r.inner.Open(target)
}

func (r *recordingProvider) count(role provider.Role) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, got := range r.roles {
		if got == role {
			n++
		}
	}
	return n
}

type failingProvider struct{}

func (failingProvider) Name() string { return "failing" }
func (failingProvider) Open(provider.Target) (*dbx.DB, error) {
	return nil, errors.New("connection refused")
}

func Greet(_ turbine.Context, name string) (string, error) {
	return "hello, " + name, nil
}

func quietTurbine() turbine.Config {
	return turbine.Config{
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		ShutdownTimeout: time.Second,
	}
}

// runGreet launches a standalone runtime on cfg and runs one durable workflow end to end.
func runGreet(t *testing.T, cfg turbinedb.Config) {
	t.Helper()
	s, err := turbinedb.NewStandalone(cfg)
	if err != nil {
		t.Fatalf("NewStandalone: %v", err)
	}
	defer s.Shutdown()

	turbine.Register(s.Runtime, Greet)
	if err := s.Launch(); err != nil {
		t.Fatalf("Launch: %v", err)
	}

	handle, err := turbine.Run(s.Runtime, Greet, "world")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	got, err := handle.GetResult()
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if got != "hello, world" {
		t.Fatalf("result = %q", got)
	}
}

func TestStandaloneDefaultProvider(t *testing.T) {
	runGreet(t, turbinedb.Config{DataDir: t.TempDir(), Turbine: quietTurbine()})
}

func TestStandaloneRoutesRolesToProviders(t *testing.T) {
	data := &recordingProvider{inner: sqlite.New(sqlite.Config{})}
	aux := &recordingProvider{inner: sqlite.New(sqlite.Config{})}

	runGreet(t, turbinedb.Config{
		DataDir:     t.TempDir(),
		Provider:    data,
		AuxProvider: aux,
		Turbine:     quietTurbine(),
	})

	// PocketBase opens two pools (concurrent + single-writer) per database.
	if n := data.count(provider.RoleData); n != 2 {
		t.Errorf("data provider opened data DB %d times, want 2", n)
	}
	if n := data.count(provider.RoleAuxiliary); n != 0 {
		t.Errorf("data provider opened aux DB %d times, want 0", n)
	}
	if n := aux.count(provider.RoleAuxiliary); n != 2 {
		t.Errorf("aux provider opened aux DB %d times, want 2", n)
	}
}

func TestStandaloneAuxDefaultsToProvider(t *testing.T) {
	p := &recordingProvider{inner: sqlite.New(sqlite.Config{})}
	runGreet(t, turbinedb.Config{DataDir: t.TempDir(), Provider: p, Turbine: quietTurbine()})

	if p.count(provider.RoleAuxiliary) == 0 {
		t.Fatal("expected Provider to also open the auxiliary DB when AuxProvider is nil")
	}
}

func TestStandaloneProviderErrorSurfaces(t *testing.T) {
	s, err := turbinedb.NewStandalone(turbinedb.Config{
		DataDir:  t.TempDir(),
		Provider: failingProvider{},
		Turbine:  quietTurbine(),
	})
	if err != nil {
		t.Fatalf("NewStandalone: %v", err)
	}
	defer s.Shutdown()

	if err := s.Launch(); err == nil {
		t.Fatal("expected Launch to fail when the provider cannot connect")
	}
}

func TestShutdownIsIdempotent(t *testing.T) {
	s, err := turbinedb.NewStandalone(turbinedb.Config{DataDir: t.TempDir(), Turbine: quietTurbine()})
	if err != nil {
		t.Fatalf("NewStandalone: %v", err)
	}
	s.Shutdown()
	s.Shutdown()
}

func TestNewAppBuildsRuntime(t *testing.T) {
	app, rt, err := turbinedb.NewApp(turbinedb.Config{DataDir: t.TempDir(), Turbine: quietTurbine()})
	if err != nil {
		t.Fatalf("NewApp: %v", err)
	}
	if app == nil || rt == nil {
		t.Fatal("NewApp returned nil app or runtime")
	}
}

func TestInvalidConfigRejected(t *testing.T) {
	_, err := turbinedb.NewStandalone(turbinedb.Config{DataMaxOpenConns: -1})
	if !errors.Is(err, turbinedb.ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}
