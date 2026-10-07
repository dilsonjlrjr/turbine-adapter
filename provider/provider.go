// Package provider defines the contract every database provider implements
// and a name-based registry used for configuration-driven setup.
//
// The package depends only on github.com/pocketbase/dbx, so provider
// implementations can live in their own Go modules without pulling in the
// rest of the adapter.
package provider

import (
	"path/filepath"

	"github.com/pocketbase/dbx"
)

// Role identifies which PocketBase database a connection is opened for.
//
// PocketBase (and therefore Turbine) keeps two SQLite-dialect databases:
// the main data database and an auxiliary one used for request logs.
type Role string

const (
	// RoleData is the main database: collections, records and every pt_* Turbine table.
	RoleData Role = "data"
	// RoleAuxiliary is the auxiliary database: PocketBase request logs.
	RoleAuxiliary Role = "auxiliary"
)

// auxiliaryFileName is the file name PocketBase uses for the auxiliary database.
const auxiliaryFileName = "auxiliary.db"

// Target describes the database PocketBase is asking a provider to open.
type Target struct {
	// Role tells whether this is the data or the auxiliary database.
	Role Role
	// Path is the filesystem path PocketBase would use by default
	// (<DataDir>/data.db or <DataDir>/auxiliary.db). Remote providers may ignore it.
	Path string
}

// TargetFromPath builds a Target from the path PocketBase hands to its DBConnect hook.
func TargetFromPath(dbPath string) Target {
	role := RoleData
	if filepath.Base(dbPath) == auxiliaryFileName {
		role = RoleAuxiliary
	}
	return Target{Role: role, Path: dbPath}
}

// Provider opens SQLite-dialect connections for PocketBase.
//
// Open is called twice per database (PocketBase keeps a concurrent pool and
// a single-connection pool for writes), so it must return a fresh *dbx.DB on
// every call. The returned DB must use the SQLite query builder; build it with
// [dbx.NewFromDB](sqlDB, "sqlite") when the driver name is not "sqlite"/"sqlite3".
type Provider interface {
	// Name returns a short, stable identifier used in logs and errors.
	Name() string
	// Open returns a new connection pool for the given target.
	Open(target Target) (*dbx.DB, error)
}
