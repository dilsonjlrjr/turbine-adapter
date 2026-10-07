// Command lab is a complete order-fulfillment demo on PostgreSQL.
//
// Everything is configured through TURBINE_* environment variables
// (see README.md); execution state lives in PostgreSQL when TURBINE_SYSDB=postgres.
//
//	TURBINE_SYSDB=postgres TURBINE_SYSDB_DSN=postgres://... go run . serve
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"strings"

	"github.com/YakirOren/turbine"
	"github.com/YakirOren/turbine/dashboard"
	"github.com/pocketbase/pocketbase/core"

	"github.com/turbine-adapter/turbinedb"
	// Execution state in PostgreSQL, selected by TURBINE_SYSDB=postgres.
	_ "github.com/turbine-adapter/turbinedb/providers/postgres"
)

// rt is the runtime, shared with workflows that start other workflows or
// read the KV store (workflow functions only receive a turbine.Context).
var rt *turbine.Runtime

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg, err := turbinedb.ConfigFromEnv()
	if err != nil {
		return err
	}
	cfg.Turbine.ProductSender = logSender{log: slog.New(slog.NewTextHandler(os.Stdout, nil))}

	app, runtime, err := turbinedb.NewApp(cfg)
	if err != nil {
		return err
	}
	rt = runtime

	registerWorkflows()
	dashboard.Mount(app, rt)
	app.OnServe().BindFunc(func(e *core.ServeEvent) error {
		if err := seedKV(); err != nil {
			return err
		}
		if err := upsertSuperuser(e.App); err != nil {
			return err
		}
		log.Printf("dashboard: %s/_/turbine/  (PocketBase admin: %s/_/)", baseURL(e), baseURL(e))
		return e.Next()
	})

	return app.Start()
}

func registerWorkflows() {
	rt.Queue(ordersQueue,
		turbine.WithWorkerConcurrency(5),
		turbine.WithGlobalConcurrency(10),
		turbine.WithPartitionQueue(), // limits apply per customer partition
	)

	turbine.Register(rt, ProcessOrder,
		turbine.WithDashboardTrigger(),
		turbine.WithTags("orders", "fulfillment"),
		turbine.WithSummaryFunc(OrderInput.summary),
		turbine.WithInputSchema(map[string]any{
			"fields": []map[string]any{
				{"name": "order_id", "type": "string", "label": "Pedido", "required": true, "placeholder": "ORD-1001"},
				{"name": "customer", "type": "string", "label": "Cliente", "required": true, "placeholder": "acme"},
				{"name": "amount", "type": "number", "label": "Valor (R$)", "required": true, "placeholder": "250"},
				{"name": "items", "type": "number", "label": "Itens", "placeholder": "1"},
			},
		}),
	)
	turbine.Register(rt, PaymentGateway, turbine.WithTags("payments"))
	turbine.Register(rt, NotifyCustomer, turbine.WithTags("notifications"))
	turbine.Register(rt, GenerateDemoOrders,
		turbine.WithSchedule("*/2 * * * *"),
		turbine.WithTags("demo"),
	)
}

// baseURL guesses the externally useful base URL of the HTTP server.
func baseURL(e *core.ServeEvent) string {
	addr := e.Server.Addr
	if strings.HasPrefix(addr, "0.0.0.0:") || strings.HasPrefix(addr, ":") {
		addr = "localhost:" + addr[strings.LastIndex(addr, ":")+1:]
	}
	return "http://" + addr
}

// seedKV creates the config/counter keys on first start.
func seedKV() error {
	ctx := context.Background()
	if _, ok, err := turbine.KVGet[float64](rt, ctx, kvApprovalThreshold); err != nil {
		return err
	} else if !ok {
		if err := rt.KVSet(ctx, kvApprovalThreshold, defaultApprovalThreshold); err != nil {
			return err
		}
	}
	if _, ok, err := turbine.KVGet[float64](rt, ctx, kvOrdersProcessed); err != nil {
		return err
	} else if !ok {
		return rt.KVSet(ctx, kvOrdersProcessed, 0)
	}
	return nil
}

// upsertSuperuser creates or updates the PocketBase superuser from
// LAB_SUPERUSER_EMAIL / LAB_SUPERUSER_PASSWORD (no-op when unset).
func upsertSuperuser(app core.App) error {
	email, password := os.Getenv("LAB_SUPERUSER_EMAIL"), os.Getenv("LAB_SUPERUSER_PASSWORD")
	if email == "" || password == "" {
		return nil
	}

	rec, err := app.FindAuthRecordByEmail(core.CollectionNameSuperusers, email)
	if err != nil {
		col, cerr := app.FindCachedCollectionByNameOrId(core.CollectionNameSuperusers)
		if cerr != nil {
			return errors.Join(err, cerr)
		}
		rec = core.NewRecord(col)
		rec.SetEmail(email)
	}
	rec.SetPassword(password)
	return app.Save(rec)
}
