package sqlite_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/turbine-adapter/turbinedb/provider"
	"github.com/turbine-adapter/turbinedb/sqlite"
)

func queryPragma(t *testing.T, p provider.Provider, pragma string) string {
	t.Helper()
	db, err := p.Open(provider.Target{Role: provider.RoleData, Path: filepath.Join(t.TempDir(), "data.db")})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	var value string
	if err := db.NewQuery("PRAGMA " + pragma).Row(&value); err != nil {
		t.Fatalf("PRAGMA %s: %v", pragma, err)
	}
	return strings.ToLower(value)
}

func TestDefaultPragmasApplied(t *testing.T) {
	p := sqlite.New(sqlite.Config{})
	if got := queryPragma(t, p, "journal_mode"); got != "wal" {
		t.Fatalf("journal_mode = %q, want wal", got)
	}
	if got := queryPragma(t, p, "foreign_keys"); got != "1" {
		t.Fatalf("foreign_keys = %q, want 1", got)
	}
}

func TestCustomPragmas(t *testing.T) {
	p := sqlite.New(sqlite.Config{Pragmas: []string{"busy_timeout(1234)"}})
	if got := queryPragma(t, p, "busy_timeout"); got != "1234" {
		t.Fatalf("busy_timeout = %q, want 1234", got)
	}
	if got := queryPragma(t, p, "journal_mode"); got == "wal" {
		t.Fatal("custom pragma list must replace the defaults")
	}
}

func TestRegisteredFromSettings(t *testing.T) {
	p, err := provider.Open(sqlite.Name, provider.Settings{
		Options: map[string]string{"pragmas": "busy_timeout(777); foreign_keys(ON)"},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if p.Name() != sqlite.Name {
		t.Fatalf("Name = %q", p.Name())
	}
	if got := queryPragma(t, p, "busy_timeout"); got != "777" {
		t.Fatalf("busy_timeout = %q, want 777", got)
	}
}
