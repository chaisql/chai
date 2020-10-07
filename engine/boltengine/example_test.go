package boltengine_test

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"path"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/engine/boltengine"
)

func Example() {
	ctx := context.Background()

	dir, err := ioutil.TempDir("", "bolt")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := genji.Open(ctx, path.Join(dir, "my.db"))
	defer db.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleNewEngine() {
	ctx := context.Background()

	dir, err := ioutil.TempDir("", "bolt")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ng, err := boltengine.NewEngine(path.Join(dir, "genji.db"), 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	db, err := genji.New(ctx, ng)
	defer db.Close()
	if err != nil {
		log.Fatal(err)
	}
}
