package libsql_test

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/YakirOren/turbine"
	"github.com/pocketbase/dbx"

	"github.com/turbine-adapter/turbinedb"
	"github.com/turbine-adapter/turbinedb/provider"
	"github.com/turbine-adapter/turbinedb/providers/libsql"
)

// Integration tests run only when a libSQL server is reachable, e.g.
//
//	docker run -p 8080:8080 ghcr.io/tursodatabase/libsql-server:latest
//	TURBINEDB_LIBSQL_URL=http://127.0.0.1:8080 go test ./...
const envTestURL = "TURBINEDB_LIBSQL_URL"

const envTestToken = "TURBINEDB_LIBSQL_AUTH_TOKEN"

type recordingLocal struct{ opened []provider.Role }

func (*recordingLocal) Name() string { return "local" }
func (r *recordingLocal) Open(t provider.Target) (*dbx.DB, error) {
	r.opened = append(r.opened, t.Role)
	return nil, errors.New("stub")
}

func TestNewValidation(t *testing.T) {
	if _, err := libsql.New(libsql.Config{}); !errors.Is(err, libsql.ErrMissingURL) {
		t.Errorf("missing URL: got %v", err)
	}
	if _, err := libsql.New(libsql.Config{URL: "http://x", AuxURL: "http://x"}); !errors.Is(err, libsql.ErrSameURL) {
		t.Errorf("same URL: got %v", err)
	}
}

func TestAuxFallsBackToLocal(t *testing.T) {
	local := &recordingLocal{}
	p, err := libsql.New(libsql.Config{URL: "http://127.0.0.1:1", LocalAux: local})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, _ = p.Open(provider.Target{Role: provider.RoleAuxiliary, Path: filepath.Join(t.TempDir(), "auxiliary.db")})
	if len(local.opened) != 1 || local.opened[0] != provider.RoleAuxiliary {
		t.Fatalf("aux DB should be opened by LocalAux, got %v", local.opened)
	}
}

func TestFromSettings(t *testing.T) {
	p, err := provider.Open(libsql.Name, provider.Settings{
		DSN:     "http://127.0.0.1:1",
		Options: map[string]string{libsql.OptionAuxURL: "http://127.0.0.1:2"},
	})
	if err != nil {
		t.Fatalf("provider.Open: %v", err)
	}
	if p.Name() != libsql.Name {
		t.Fatalf("Name = %q", p.Name())
	}
	if _, err := provider.Open(libsql.Name, provider.Settings{}); !errors.Is(err, libsql.ErrMissingURL) {
		t.Fatalf("expected ErrMissingURL, got %v", err)
	}
}

func Greet(_ turbine.Context, name string) (string, error) {
	return "hello, " + name, nil
}

func TestIntegrationRunsTurbineWorkflow(t *testing.T) {
	url := os.Getenv(envTestURL)
	if url == "" {
		t.Skipf("set %s to run against a libSQL server", envTestURL)
	}

	p, err := libsql.New(libsql.Config{URL: url, AuthToken: os.Getenv(envTestToken)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s, err := turbinedb.NewStandalone(turbinedb.Config{
		DataDir:  t.TempDir(),
		Provider: p,
		Turbine: turbine.Config{
			Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
			ShutdownTimeout: time.Second,
		},
	})
	if err != nil {
		t.Fatalf("NewStandalone: %v", err)
	}
	defer s.Shutdown()

	turbine.Register(s.Runtime, Greet)
	if err := s.Launch(); err != nil {
		t.Fatalf("Launch: %v", err)
	}
	handle, err := turbine.Run(s.Runtime, Greet, "libsql")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, err := handle.GetResult(); err != nil || got != "hello, libsql" {
		t.Fatalf("GetResult = %q, %v", got, err)
	}
}
