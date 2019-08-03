package badger_test

import (
	"io/ioutil"
	"log"
	"os"
	"path"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/badger"
	bdg "github.com/dgraph-io/badger"
)

func Example() {
	dir, err := ioutil.TempDir("", "badger")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ng, err := badger.NewEngine(bdg.DefaultOptions(path.Join(dir, "badger")))
	if err != nil {
		log.Fatal(err)
	}

	db := genji.New(ng)
	defer db.Close()
}
