package provider

import (
	"errors"
	"fmt"
	"slices"
	"sync"
)

// ErrUnknownProvider is returned by Open when no factory is registered under the requested name.
var ErrUnknownProvider = errors.New("provider: unknown provider")

// Settings is the provider-agnostic configuration handed to a Factory.
// Each provider documents which fields and options it understands.
type Settings struct {
	// DSN is the provider-specific connection string (URL, file path, ...).
	DSN string
	// AuthToken is an optional credential for remote providers.
	AuthToken string
	// Options holds extra provider-specific key/value settings.
	Options map[string]string
}

// Option returns Options[key] or fallback when the key is missing or empty.
func (s Settings) Option(key, fallback string) string {
	if v, ok := s.Options[key]; ok && v != "" {
		return v
	}
	return fallback
}

// Factory builds a Provider from generic Settings.
type Factory func(settings Settings) (Provider, error)

type registry struct {
	mu        sync.RWMutex
	factories map[string]Factory
}

//nolint:gochecknoglobals // process-wide registry, same pattern as database/sql drivers.
var defaultRegistry = &registry{factories: map[string]Factory{}}

// Register makes a provider factory available under name.
// It is meant to be called from a provider package's init function and
// panics when name is empty, factory is nil or name is already taken.
func Register(name string, factory Factory) {
	if name == "" {
		panic("provider: Register called with empty name")
	}
	if factory == nil {
		panic("provider: Register called with nil factory for " + name)
	}

	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()

	if _, dup := defaultRegistry.factories[name]; dup {
		panic("provider: Register called twice for " + name)
	}
	defaultRegistry.factories[name] = factory
}

// Open builds the provider registered under name.
func Open(name string, settings Settings) (Provider, error) {
	defaultRegistry.mu.RLock()
	factory, ok := defaultRegistry.factories[name]
	defaultRegistry.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w %q (registered: %v); did you blank-import its package?",
			ErrUnknownProvider, name, Registered())
	}

	p, err := factory(settings)
	if err != nil {
		return nil, fmt.Errorf("provider %q: %w", name, err)
	}
	return p, nil
}

// Registered returns the sorted names of every registered provider.
func Registered() []string {
	defaultRegistry.mu.RLock()
	defer defaultRegistry.mu.RUnlock()

	names := make([]string, 0, len(defaultRegistry.factories))
	for name := range defaultRegistry.factories {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}
