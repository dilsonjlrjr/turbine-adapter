package postgres_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"

	"github.com/turbine-adapter/turbinedb/providers/postgres"
)

// dsnEnv selects an external PostgreSQL; when unset an embedded one is started.
const dsnEnv = "TURBINEDB_POSTGRES_DSN"

// testDSN is the DSN every test connects to (each test uses its own schema).
var testDSN string

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	if dsn := os.Getenv(dsnEnv); dsn != "" {
		testDSN = dsn
		return m.Run()
	}

	dir, err := os.MkdirTemp("", "turbinedb-pg-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, "tempdir:", err)
		return 1
	}
	defer os.RemoveAll(dir)

	port, err := freePort()
	if err != nil {
		fmt.Fprintln(os.Stderr, "free port:", err)
		return 1
	}
	pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Version(embeddedpostgres.V17).
		Port(port).
		DataPath(dir + "/data").
		RuntimePath(dir + "/runtime").
		Logger(io.Discard))
	if err := pg.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "embedded postgres:", err)
		return 1
	}
	defer func() { _ = pg.Stop() }()

	testDSN = "postgres://postgres:postgres@127.0.0.1:" + strconv.Itoa(int(port)) + "/postgres?sslmode=disable"
	return m.Run()
}

func freePort() (uint32, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return uint32(l.Addr().(*net.TCPAddr).Port), nil
}

func randomSchema() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return "t_" + hex.EncodeToString(b)
}

// dropSchema removes a test schema through a separate connection, so it works
// after the DB under test was shut down.
func dropSchema(t *testing.T, schema string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, testDSN)
	if err != nil {
		t.Errorf("drop schema: connect: %v", err)
		return
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`); err != nil {
		t.Errorf("drop schema %s: %v", schema, err)
	}
}

// openSchema opens a DB on the given schema (created if missing). It does not
// register any cleanup.
func openSchema(t *testing.T, schema string, mut func(*postgres.Config)) *postgres.DB {
	t.Helper()
	cfg := postgres.Config{DSN: testDSN, Schema: schema}
	if mut != nil {
		mut(&cfg)
	}
	db, err := postgres.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("postgres.Open: %v", err)
	}
	return db
}

func postgresOpen(schema string) (*postgres.DB, error) {
	return postgresOpenCfg(schema, testDSN)
}

func postgresOpenCfg(schema, dsn string) (*postgres.DB, error) {
	return postgres.Open(context.Background(), postgres.Config{DSN: dsn, Schema: schema})
}

func adminConn(t *testing.T) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}
