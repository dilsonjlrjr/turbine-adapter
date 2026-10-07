package postgres_test

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/YakirOren/turbine"
	"github.com/jackc/pgx/v5"

	"github.com/turbine-adapter/turbinedb"
)

func quietTurbine() turbine.Config {
	return turbine.Config{
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		ShutdownTimeout: 5 * time.Second,
	}
}

func pgCount(t *testing.T, conn *pgx.Conn, schema, table string) int {
	t.Helper()
	var n int
	if err := conn.QueryRow(context.Background(), `SELECT count(*) FROM "`+schema+`".`+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func TestStandaloneEndToEnd(t *testing.T) {
	ctx := context.Background()
	schema := randomSchema()
	t.Cleanup(func() { dropSchema(t, schema) })
	db := openSchema(t, schema, nil)

	tcfg := quietTurbine()
	tcfg.SystemDatabase = db
	s, err := turbinedb.NewStandalone(turbinedb.Config{DataDir: t.TempDir(), Turbine: tcfg})
	if err != nil {
		t.Fatalf("NewStandalone: %v", err)
	}
	defer s.Shutdown()
	rt := s.Runtime

	childWF := func(_ turbine.Context, in string) (string, error) { return "child:" + in, nil }
	receiverWF := func(c turbine.Context, _ string) (string, error) {
		return turbine.Recv[string](c, "greeting", 10*time.Second)
	}
	queuedWF := func(_ turbine.Context, n int) (int, error) { return n * 2, nil }
	parentWF := func(c turbine.Context, in string) (string, error) {
		upper, err := turbine.Do(c, func(context.Context) (string, error) { return strings.ToUpper(in), nil }, turbine.WithStepName("upper"))
		if err != nil {
			return "", err
		}
		size, err := turbine.Do(c, func(context.Context) (int, error) { return len(upper), nil }, turbine.WithStepName("size"))
		if err != nil {
			return "", err
		}
		h, err := turbine.Run(rt, childWF, upper)
		if err != nil {
			return "", err
		}
		childOut, err := h.GetResult()
		if err != nil {
			return "", err
		}
		if err := turbine.Send(c, "receiver-wf", "hi from parent", "greeting"); err != nil {
			return "", err
		}
		if err := turbine.SetValue(c, "progress", size); err != nil {
			return "", err
		}
		return childOut, nil
	}

	turbine.Register(rt, childWF)
	turbine.Register(rt, receiverWF)
	turbine.Register(rt, queuedWF)
	turbine.Register(rt, parentWF)
	rt.Queue("e2e-queue", turbine.WithGlobalConcurrency(2))

	if err := s.Launch(); err != nil {
		t.Fatalf("Launch: %v", err)
	}

	recvH, err := turbine.Run(rt, receiverWF, "", turbine.WithID("receiver-wf"))
	if err != nil {
		t.Fatal(err)
	}
	parentH, err := turbine.Run(rt, parentWF, "abc", turbine.WithID("parent-wf"))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := parentH.GetResult(); err != nil || got != "child:ABC" {
		t.Fatalf("parent = %q, %v", got, err)
	}
	if got, err := recvH.GetResult(); err != nil || got != "hi from parent" {
		t.Fatalf("receiver = %q, %v", got, err)
	}

	var queued []turbine.Handle[int]
	for i := 1; i <= 5; i++ {
		h, err := turbine.Run(rt, queuedWF, i, turbine.WithQueue("e2e-queue"))
		if err != nil {
			t.Fatal(err)
		}
		queued = append(queued, h)
	}
	for i, h := range queued {
		if got, err := h.GetResult(); err != nil || got != (i+1)*2 {
			t.Fatalf("queued[%d] = %d, %v", i, got, err)
		}
	}

	if err := rt.KVSet(ctx, "cfg", map[string]any{"a": 1}); err != nil {
		t.Fatal(err)
	}
	kv, ok, err := turbine.KVGet[map[string]any](rt, ctx, "cfg")
	if err != nil || !ok || kv["a"] != float64(1) {
		t.Fatalf("KVGet = %v, %v, %v", kv, ok, err)
	}
	progress, err := turbine.GetValue[int](rt.NewContext(ctx), "parent-wf", "progress", time.Second)
	if err != nil || progress != 3 {
		t.Fatalf("progress event = %d, %v", progress, err)
	}

	// Execution state is in PostgreSQL ...
	conn := adminConn(t)
	for table, min := range map[string]int{
		"workflow_status": 8, "operation_outputs": 2, "notifications": 1, "workflow_events": 1, "kv": 1,
	} {
		if n := pgCount(t, conn, schema, table); n < min {
			t.Errorf("%s.%s has %d rows, want >= %d", schema, table, n, min)
		}
	}
	// ... and the PocketBase execution collections stay empty.
	for _, table := range []string{"pt_workflow_status", "pt_operation_outputs", "pt_notifications", "pt_workflow_events", "pt_kv"} {
		var n int
		if err := s.App.DB().NewQuery("SELECT COUNT(*) FROM " + table).Row(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if n != 0 {
			t.Errorf("PocketBase %s has %d rows, want 0", table, n)
		}
	}
}

func TestEnvConfigBuildsWorkingRuntime(t *testing.T) {
	schema := randomSchema()
	t.Cleanup(func() { dropSchema(t, schema) })

	env := map[string]string{
		turbinedb.EnvSysDB:        "postgres",
		turbinedb.EnvSysDBDSN:     testDSN,
		turbinedb.EnvSysDBOptions: "schema=" + schema + ",max_conns=6,poll_interval=250ms",
		turbinedb.EnvDataDir:      t.TempDir(),
	}
	cfg, err := turbinedb.ConfigFromLookup(func(k string) (string, bool) { v, ok := env[k]; return v, ok })
	if err != nil {
		t.Fatalf("ConfigFromLookup: %v", err)
	}
	if cfg.Turbine.SystemDatabase == nil {
		t.Fatal("SystemDatabase not set from TURBINE_SYSDB")
	}
	q := quietTurbine()
	cfg.Turbine.Logger, cfg.Turbine.ShutdownTimeout = q.Logger, q.ShutdownTimeout

	s, err := turbinedb.NewStandalone(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown()
	greet := func(_ turbine.Context, name string) (string, error) { return "hello, " + name, nil }
	turbine.Register(s.Runtime, greet)
	if err := s.Launch(); err != nil {
		t.Fatal(err)
	}
	h, err := turbine.Run(s.Runtime, greet, "pg")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := h.GetResult(); err != nil || got != "hello, pg" {
		t.Fatalf("result = %q, %v", got, err)
	}
	if n := pgCount(t, adminConn(t), schema, "workflow_status"); n != 1 {
		t.Fatalf("workflow_status rows = %d, want 1", n)
	}
}

func TestEnvConfigErrors(t *testing.T) {
	for name, opts := range map[string]string{
		"bad max_conns":     "max_conns=abc",
		"bad poll_interval": "poll_interval=soon",
		"bad schema":        "schema=a-b",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := turbinedb.ConfigFromLookup(func(k string) (string, bool) {
				v, ok := map[string]string{
					turbinedb.EnvSysDB: "postgres", turbinedb.EnvSysDBDSN: testDSN, turbinedb.EnvSysDBOptions: opts,
				}[k]
				return v, ok
			})
			if err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}
