package mattn_test

import (
	"io"
	"log/slog"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/YakirOren/turbine"

	"github.com/turbine-adapter/turbinedb"
	"github.com/turbine-adapter/turbinedb/provider"
	"github.com/turbine-adapter/turbinedb/providers/mattn"
)

func Greet(_ turbine.Context, name string) (string, error) {
	return "hello, " + name, nil
}

func TestParamsApplied(t *testing.T) {
	p := mattn.New(mattn.Config{Params: url.Values{"_busy_timeout": {"4321"}}})
	db, err := p.Open(provider.Target{Role: provider.RoleData, Path: filepath.Join(t.TempDir(), "data.db")})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	for pragma, want := range map[string]string{"busy_timeout": "4321", "journal_mode": "wal"} {
		var got string
		if err := db.NewQuery("PRAGMA " + pragma).Row(&got); err != nil {
			t.Fatalf("PRAGMA %s: %v", pragma, err)
		}
		if strings.ToLower(got) != want {
			t.Errorf("%s = %q, want %q", pragma, got, want)
		}
	}
}

func TestRunsTurbineWorkflow(t *testing.T) {
	p, err := provider.Open(mattn.Name, provider.Settings{})
	if err != nil {
		t.Fatalf("provider.Open: %v", err)
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
	handle, err := turbine.Run(s.Runtime, Greet, "mattn")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, err := handle.GetResult(); err != nil || got != "hello, mattn" {
		t.Fatalf("GetResult = %q, %v", got, err)
	}
}
