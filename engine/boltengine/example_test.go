package boltengine_test

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/engine/boltengine"
)

func Example() {
	dir, err := ioutil.TempDir("", "bolt")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := genji.Open(filepath.Join(dir, "my.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}

func ExampleNewEngine() {
	dir, err := ioutil.TempDir("", "bolt")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ng, err := boltengine.NewEngine(filepath.Join(dir, "genji.db"), 0o600, nil)
	if err != nil {
		log.Fatal(err)
	}

	db, err := genji.New(context.Background(), ng)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}
