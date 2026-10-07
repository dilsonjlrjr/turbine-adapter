// Command fromenv picks the provider entirely from TURBINE_* environment variables.
//
//	go run .                                                        # local SQLite
//	TURBINE_DB_PROVIDER=libsql TURBINE_DB_DSN=http://127.0.0.1:8080 go run .
package main

import (
	"log"

	"github.com/YakirOren/turbine"

	"github.com/turbine-adapter/turbinedb"
	// Link the providers this binary may select at runtime.
	_ "github.com/turbine-adapter/turbinedb/providers/libsql"
)

func Greet(_ turbine.Context, name string) (string, error) {
	return "hello, " + name, nil
}

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

	s, err := turbinedb.NewStandalone(cfg)
	if err != nil {
		return err
	}
	defer s.Shutdown()

	turbine.Register(s.Runtime, Greet)
	if err := s.Launch(); err != nil {
		return err
	}

	handle, err := turbine.Run(s.Runtime, Greet, "env")
	if err != nil {
		return err
	}
	result, err := handle.GetResult()
	if err != nil {
		return err
	}
	log.Println(result)
	return nil
}
