package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/YakirOren/turbine"
	"github.com/YakirOren/turbine/sysdbtest"
)

// TestConformance runs the SystemDatabase conformance suite against a fresh
// schema per subtest. RunSuite itself calls Launch and Shutdown.
func TestConformance(t *testing.T) {
	sysdbtest.RunSuite(t, func(t *testing.T) turbine.SystemDatabase {
		schema := randomSchema()
		t.Cleanup(func() { dropSchema(t, schema) })
		return openSchema(t, schema, nil)
	})
}

func TestMigrationsIdempotentAndConcurrent(t *testing.T) {
	schema := randomSchema()
	t.Cleanup(func() { dropSchema(t, schema) })

	// Several processes (here: instances) starting at once serialize on the
	// advisory lock; every one must succeed.
	const n = 4
	errs := make(chan error, n)
	dbs := make(chan interface {
		Shutdown(context.Context, time.Duration)
	}, n)
	for range n {
		go func() {
			db, err := postgresOpen(schema)
			if err == nil {
				dbs <- db
			}
			errs <- err
		}()
	}
	for range n {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent Open: %v", err)
		}
	}
	for range n {
		(<-dbs).Shutdown(context.Background(), time.Second)
	}

	conn := adminConn(t)
	var count int
	if err := conn.QueryRow(context.Background(), `SELECT count(*) FROM "`+schema+`".schema_migrations`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("schema_migrations rows = %d, want 1", count)
	}
}

func TestInvalidConfig(t *testing.T) {
	for name, schema := range map[string]string{"quote": `a"b`, "space": "a b", "digit first": "1a", "long": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {
		t.Run(name, func(t *testing.T) {
			if _, err := postgresOpenCfg(schema, testDSN); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
	if _, err := postgresOpenCfg("", ""); err == nil {
		t.Fatal("expected an error for an empty DSN")
	}
}
