package badgerengine_test

import (
	"io/ioutil"
	"log"
	"os"
	"path"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/badgerengine"
	"github.com/dgraph-io/badger/v2"
)

func Example() {
	dir, err := ioutil.TempDir("", "badger")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ng, err := badgerengine.NewEngine(badger.DefaultOptions(path.Join(dir, "badger")))
	if err != nil {
		log.Fatal(err)
	}

	db, err := genji.Open(ng)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}
