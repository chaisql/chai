package kv_test

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/internal/kv"
)

func Example() {
	dir, err := ioutil.TempDir("", "badger")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ng, err := kv.NewEngine(badger.DefaultOptions(filepath.Join(dir, "badger")))
	if err != nil {
		log.Fatal(err)
	}

	db, err := genji.New(context.Background(), ng)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}
