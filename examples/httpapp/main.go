// Command httpapp serves the PocketBase HTTP API + Turbine on a libSQL database.
//
//	LIBSQL_URL=libsql://my-db.turso.io LIBSQL_AUTH_TOKEN=... go run . serve
package main

import (
	"log"
	"os"

	"github.com/YakirOren/turbine"

	"github.com/turbine-adapter/turbinedb"
	"github.com/turbine-adapter/turbinedb/providers/libsql"
)

func Greet(_ turbine.Context, name string) (string, error) {
	return "hello, " + name, nil
}

func main() {
	p, err := libsql.New(libsql.Config{
		URL:       os.Getenv("LIBSQL_URL"),
		AuthToken: os.Getenv("LIBSQL_AUTH_TOKEN"),
	})
	if err != nil {
		log.Fatal(err)
	}

	app, rt, err := turbinedb.NewApp(turbinedb.Config{Provider: p})
	if err != nil {
		log.Fatal(err)
	}

	turbine.Register(rt, Greet, turbine.WithDashboardTrigger())

	if err := app.Start(); err != nil {
		log.Fatal(err)
	}
}
