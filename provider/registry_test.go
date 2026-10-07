package provider_test

import (
	"errors"
	"testing"

	"github.com/pocketbase/dbx"

	"github.com/turbine-adapter/turbinedb/provider"
)

type stubProvider struct{ settings provider.Settings }

func (*stubProvider) Name() string { return "stub" }
func (*stubProvider) Open(provider.Target) (*dbx.DB, error) {
	return nil, errors.New("not implemented")
}

func TestRegisterAndOpen(t *testing.T) {
	provider.Register("registry-test-stub", func(s provider.Settings) (provider.Provider, error) {
		return &stubProvider{settings: s}, nil
	})

	p, err := provider.Open("registry-test-stub", provider.Settings{DSN: "x://y", AuthToken: "tok"})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	stub, ok := p.(*stubProvider)
	if !ok {
		t.Fatalf("unexpected provider type %T", p)
	}
	if stub.settings.DSN != "x://y" || stub.settings.AuthToken != "tok" {
		t.Fatalf("settings not forwarded: %+v", stub.settings)
	}
}

func TestOpenUnknownProvider(t *testing.T) {
	_, err := provider.Open("does-not-exist", provider.Settings{})
	if !errors.Is(err, provider.ErrUnknownProvider) {
		t.Fatalf("expected ErrUnknownProvider, got %v", err)
	}
}

func TestOpenWrapsFactoryError(t *testing.T) {
	boom := errors.New("boom")
	provider.Register("registry-test-failing", func(provider.Settings) (provider.Provider, error) {
		return nil, boom
	})
	if _, err := provider.Open("registry-test-failing", provider.Settings{}); !errors.Is(err, boom) {
		t.Fatalf("expected wrapped factory error, got %v", err)
	}
}

func TestRegisterPanics(t *testing.T) {
	factory := func(provider.Settings) (provider.Provider, error) { return &stubProvider{}, nil }
	provider.Register("registry-test-dup", factory)

	cases := map[string]func(){
		"empty name":  func() { provider.Register("", factory) },
		"nil factory": func() { provider.Register("registry-test-nil", nil) },
		"duplicate":   func() { provider.Register("registry-test-dup", factory) },
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			fn()
		})
	}
}

func TestTargetFromPath(t *testing.T) {
	cases := map[string]provider.Role{
		"pb_data/data.db":           provider.RoleData,
		"/abs/pb_data/auxiliary.db": provider.RoleAuxiliary,
		"other.db":                  provider.RoleData,
	}
	for path, want := range cases {
		if got := provider.TargetFromPath(path); got.Role != want || got.Path != path {
			t.Errorf("TargetFromPath(%q) = %+v, want role %s", path, got, want)
		}
	}
}

func TestSettingsOption(t *testing.T) {
	s := provider.Settings{Options: map[string]string{"a": "1", "empty": ""}}
	if got := s.Option("a", "x"); got != "1" {
		t.Errorf("Option(a) = %q", got)
	}
	if got := s.Option("empty", "x"); got != "x" {
		t.Errorf("Option(empty) = %q, want fallback", got)
	}
	if got := s.Option("missing", "x"); got != "x" {
		t.Errorf("Option(missing) = %q, want fallback", got)
	}
}
