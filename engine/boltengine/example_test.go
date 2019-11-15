package boltengine_test

import (
	"io/ioutil"
	"log"
	"os"
	"path"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/boltengine"
)

func Example() {
	dir, err := ioutil.TempDir("", "bolt")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ng, err := boltengine.NewEngine(path.Join(dir, "genji.db"), 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	db, err := genji.Open(ng)
	defer db.Close()
	if err != nil {
		log.Fatal(err)
	}
}
