// Command standalone runs a durable workflow on local SQLite with tuned pragmas.
package main

import (
	"log"

	"github.com/YakirOren/turbine"

	"github.com/turbine-adapter/turbinedb"
	"github.com/turbine-adapter/turbinedb/sqlite"
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
	s, err := turbinedb.NewStandalone(turbinedb.Config{
		DataDir: "pb_data",
		Provider: sqlite.New(sqlite.Config{
			Pragmas: append(sqlite.DefaultPragmas(), "mmap_size(268435456)"),
		}),
	})
	if err != nil {
		return err
	}
	defer s.Shutdown()

	turbine.Register(s.Runtime, Greet)
	if err := s.Launch(); err != nil {
		return err
	}

	handle, err := turbine.Run(s.Runtime, Greet, "world")
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
