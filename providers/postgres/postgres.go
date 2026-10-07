// Package postgres stores the Turbine workflow execution state (workflow
// statuses, step checkpoints, queues, messages, events and the key/value
// store) in PostgreSQL, by implementing turbine.SystemDatabase on top of
// github.com/jackc/pgx/v5.
//
// Plug it into a turbinedb configuration through turbine.Config.SystemDatabase:
//
//	db, err := postgres.Open(ctx, postgres.Config{DSN: "postgres://user:pass@host/db"})
//	if err != nil { ... }
//	cfg := turbinedb.Config{Turbine: turbine.Config{SystemDatabase: db}}
//
// The Turbine runtime calls Launch and Shutdown on the database itself, so
// callers must not. Shutdown stops the notification listener and closes the
// pool when Open created it. Importing this package also registers it as
// "postgres" in the turbinedb system database registry
// (TURBINE_SYSDB=postgres).
package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/YakirOren/turbine"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/turbine-adapter/turbinedb"
)

// Name is the registry name of this system database.
const Name = "postgres"

// Defaults applied by Open and New.
const (
	DefaultSchema       = "turbine"
	DefaultPollInterval = time.Second
)

// ErrInvalidConfig wraps every configuration validation error.
var ErrInvalidConfig = errors.New("postgres: invalid config")

var schemaPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,47}$`)

// Config configures the PostgreSQL system database.
type Config struct {
	// DSN is the connection string, for example
	// postgres://user:pass@host:5432/db?sslmode=disable. Required by Open and
	// ignored by New.
	DSN string
	// Schema holds every table; it is created when missing. Default "turbine".
	// Must be a plain identifier ([A-Za-z_][A-Za-z0-9_]*, at most 48 characters).
	Schema string
	// MaxConns caps the connections of the pool created by Open; 0 uses the
	// pgx default. One connection is held permanently by the LISTEN loop.
	// Ignored by New.
	MaxConns int32
	// PollInterval is the fallback wake-up of blocking waits (Recv, GetEvent,
	// AwaitWorkflowResult) and of the channels returned by WaitForEnqueue, so a
	// missed notification or an expired rate limit never stalls work.
	// Default 1s.
	PollInterval time.Duration
	// Logger is optional; the default is slog.Default().
	Logger *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.Schema == "" {
		c.Schema = DefaultSchema
	}
	if c.PollInterval <= 0 {
		c.PollInterval = DefaultPollInterval
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}

func (c Config) validate() error {
	var errs []error
	if !schemaPattern.MatchString(c.Schema) {
		errs = append(errs, fmt.Errorf("schema %q is not a valid identifier", c.Schema))
	}
	if c.MaxConns < 0 {
		errs = append(errs, errors.New("MaxConns must not be negative"))
	}
	if len(errs) > 0 {
		return fmt.Errorf("%w: %w", ErrInvalidConfig, errors.Join(errs...))
	}
	return nil
}

// DB is a turbine.SystemDatabase backed by PostgreSQL. It is safe for
// concurrent use and may be shared by several processes pointing at the same
// schema.
type DB struct {
	pool    *pgxpool.Pool
	ownPool bool
	cfg     Config
	log     *slog.Logger
	bus     *bus
	sql     *strings.Replacer
	channel string

	mu       sync.Mutex
	started  bool
	stopped  bool
	cancel   context.CancelFunc
	listenWG sync.WaitGroup
}

var _ turbine.SystemDatabase = (*DB)(nil)

// Open connects to PostgreSQL, runs the schema migrations and starts the
// LISTEN loop. The returned DB owns its pool and closes it on Shutdown.
func Open(ctx context.Context, cfg Config) (*DB, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.DSN == "" {
		return nil, fmt.Errorf("%w: DSN is required", ErrInvalidConfig)
	}
	pcfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("%w: parse DSN: %w", ErrInvalidConfig, err)
	}
	if cfg.MaxConns > 0 {
		pcfg.MaxConns = cfg.MaxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	db, err := newDB(ctx, pool, cfg, true)
	if err != nil {
		pool.Close()
		return nil, err
	}
	return db, nil
}

// New wraps a caller-owned pool: it runs the migrations and starts the LISTEN
// loop, but Shutdown does not close the pool. The pool needs at least two
// connections (one is held by the LISTEN loop). cfg.DSN and cfg.MaxConns are
// ignored.
func New(pool *pgxpool.Pool, cfg Config) (*DB, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if pool == nil {
		return nil, fmt.Errorf("%w: pool is nil", ErrInvalidConfig)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return newDB(ctx, pool, cfg, false)
}

func newDB(ctx context.Context, pool *pgxpool.Pool, cfg Config, own bool) (*DB, error) {
	q := quoteIdent(cfg.Schema) + "."
	db := &DB{
		pool:    pool,
		ownPool: own,
		cfg:     cfg,
		log:     cfg.Logger.With("service", "sysdb_postgres", "schema", cfg.Schema),
		bus:     newBus(),
		channel: cfg.Schema + "_turbine",
		sql: strings.NewReplacer(
			"{ws}", q+"workflow_status",
			"{oo}", q+"operation_outputs",
			"{nt}", q+"notifications",
			"{ev}", q+"workflow_events",
			"{eh}", q+"workflow_events_history",
			"{kv}", q+"kv",
		),
	}
	if err := db.migrate(ctx); err != nil {
		return nil, err
	}
	db.start()
	return db, nil
}

// Launch implements turbine.SystemDatabase. The notification listener is
// already running after Open/New, so this only makes sure it is started.
func (d *DB) Launch(_ context.Context) {
	d.start()
	d.log.Debug("PostgreSQL system database launched")
}

// Shutdown implements turbine.SystemDatabase. It stops the LISTEN loop, waits
// for it (bounded by timeout) and closes the pool when Open created it.
func (d *DB) Shutdown(ctx context.Context, timeout time.Duration) {
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		return
	}
	d.stopped = true
	cancel := d.cancel
	d.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	done := make(chan struct{})
	go func() {
		d.listenWG.Wait()
		close(done)
	}()
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	select {
	case <-done:
	case <-time.After(timeout):
		d.log.Warn("timed out waiting for the notification listener to stop")
	case <-ctx.Done():
	}
	d.bus.close()
	if d.ownPool {
		d.pool.Close()
	}
	d.log.Debug("PostgreSQL system database shut down")
}

// q expands the {table} placeholders of a query to schema-qualified names.
func (d *DB) q(query string) string { return d.sql.Replace(query) }

func quoteIdent(s string) string { return pgx.Identifier{s}.Sanitize() }

// registry factory ------------------------------------------------------------

// FromSettings builds a DB from registry settings. DSN is the connection
// string; the options understood are "schema", "max_conns" and
// "poll_interval" (a Go duration).
func FromSettings(ctx context.Context, s turbinedb.Settings) (turbine.SystemDatabase, error) {
	cfg := Config{DSN: s.DSN, Schema: s.Option("schema", "")}
	if v := s.Option("max_conns", ""); v != "" {
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("%w: max_conns: %w", ErrInvalidConfig, err)
		}
		cfg.MaxConns = int32(n)
	}
	if v := s.Option("poll_interval", ""); v != "" {
		dur, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("%w: poll_interval: %w", ErrInvalidConfig, err)
		}
		cfg.PollInterval = dur
	}
	return Open(ctx, cfg)
}

//nolint:gochecknoinits // registry self-registration, same pattern as database/sql drivers.
func init() {
	turbinedb.RegisterSystemDatabase(Name, FromSettings)
}
