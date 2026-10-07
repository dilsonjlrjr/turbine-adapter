package postgres

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type migration struct {
	version int64
	name    string
	sql     string
}

func loadMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return nil, err
	}
	var out []migration
	for _, e := range entries {
		base := strings.TrimSuffix(e.Name(), ".sql")
		num, name, _ := strings.Cut(base, "_")
		v, err := strconv.ParseInt(num, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("migration %q: version prefix must be a number: %w", e.Name(), err)
		}
		body, err := migrationFiles.ReadFile("migrations/" + e.Name())
		if err != nil {
			return nil, err
		}
		out = append(out, migration{version: v, name: name, sql: string(body)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

// migrate applies the pending migrations. A session advisory lock serializes
// concurrent processes: the second one waits, then finds nothing to do.
func (d *DB) migrate(ctx context.Context) error {
	migs, err := loadMigrations()
	if err != nil {
		return fmt.Errorf("postgres: load migrations: %w", err)
	}

	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("postgres: acquire connection for migrations: %w", err)
	}
	defer conn.Release()

	lockKey := d.cfg.Schema + ":turbine:migrate"
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(hashtextextended($1, 0))", lockKey); err != nil {
		return fmt.Errorf("postgres: migration lock: %w", err)
	}
	defer func() {
		// Best effort: closing the session releases the lock anyway.
		if _, err := conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock(hashtextextended($1, 0))", lockKey); err != nil {
			d.log.Warn("failed to release the migration lock", "error", err)
		}
	}()

	schema := quoteIdent(d.cfg.Schema)
	if _, err := conn.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+schema); err != nil {
		return fmt.Errorf("postgres: create schema: %w", err)
	}
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+schema+`.schema_migrations (
		version    BIGINT PRIMARY KEY,
		name       TEXT NOT NULL,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return fmt.Errorf("postgres: create schema_migrations: %w", err)
	}

	applied := map[int64]bool{}
	rows, err := conn.Query(ctx, "SELECT version FROM "+schema+".schema_migrations")
	if err != nil {
		return fmt.Errorf("postgres: read schema_migrations: %w", err)
	}
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return err
		}
		applied[v] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, m := range migs {
		if applied[m.version] {
			continue
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		// Migrations use unqualified names; they land in the configured schema.
		if _, err := tx.Exec(ctx, "SELECT set_config('search_path', $1, true)", schema); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		// The simple protocol allows several statements in one call.
		if _, err := tx.Conn().PgConn().Exec(ctx, m.sql).ReadAll(); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("postgres: migration %04d_%s: %w", m.version, m.name, err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO "+schema+".schema_migrations (version, name) VALUES ($1, $2)", m.version, m.name); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		d.log.Info("applied migration", "version", m.version, "name", m.name)
	}
	return nil
}
