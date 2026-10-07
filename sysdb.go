package turbinedb

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/YakirOren/turbine"
)

// ErrUnknownSystemDatabase is returned by OpenSystemDatabase when no factory is
// registered under the requested name.
var ErrUnknownSystemDatabase = errors.New("turbinedb: unknown system database")

// SystemDatabaseFactory builds a turbine.SystemDatabase (the storage of
// workflow execution state) from generic Settings. Each implementation
// documents which fields and options it understands.
//
// The factory may connect to the database and run its own migrations; ctx
// bounds that work. The returned value is handed to the Turbine runtime through
// turbine.Config.SystemDatabase, and the runtime calls Launch and Shutdown on it.
type SystemDatabaseFactory func(ctx context.Context, s Settings) (turbine.SystemDatabase, error)

type sysDBRegistry struct {
	mu        sync.RWMutex
	factories map[string]SystemDatabaseFactory
}

//nolint:gochecknoglobals // process-wide registry, same pattern as provider.Register.
var defaultSysDBRegistry = &sysDBRegistry{factories: map[string]SystemDatabaseFactory{}}

// RegisterSystemDatabase makes a system database factory available under name.
// It is meant to be called from an implementation package's init function and
// panics when name is empty, factory is nil or name is already taken.
func RegisterSystemDatabase(name string, f SystemDatabaseFactory) {
	if name == "" {
		panic("turbinedb: RegisterSystemDatabase called with empty name")
	}
	if f == nil {
		panic("turbinedb: RegisterSystemDatabase called with nil factory for " + name)
	}

	defaultSysDBRegistry.mu.Lock()
	defer defaultSysDBRegistry.mu.Unlock()

	if _, dup := defaultSysDBRegistry.factories[name]; dup {
		panic("turbinedb: RegisterSystemDatabase called twice for " + name)
	}
	defaultSysDBRegistry.factories[name] = f
}

// OpenSystemDatabase builds the system database registered under name.
func OpenSystemDatabase(ctx context.Context, name string, settings Settings) (turbine.SystemDatabase, error) {
	defaultSysDBRegistry.mu.RLock()
	f, ok := defaultSysDBRegistry.factories[name]
	defaultSysDBRegistry.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w %q (registered: %v); did you blank-import its package?",
			ErrUnknownSystemDatabase, name, RegisteredSystemDatabases())
	}

	db, err := f(ctx, settings)
	if err != nil {
		return nil, fmt.Errorf("system database %q: %w", name, err)
	}
	return db, nil
}

// RegisteredSystemDatabases returns the sorted names of every registered system database.
func RegisteredSystemDatabases() []string {
	defaultSysDBRegistry.mu.RLock()
	defer defaultSysDBRegistry.mu.RUnlock()

	names := make([]string, 0, len(defaultSysDBRegistry.factories))
	for name := range defaultSysDBRegistry.factories {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}
