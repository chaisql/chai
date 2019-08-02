package bolt_test

import (
	"io/ioutil"
	"log"
	"os"
	"path"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/bolt"
)


func Example() {
	dir, err := ioutil.TempDir("", "bolt")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ng, err := bolt.NewEngine(path.Join(dir, "genji.db"), 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	db := genji.New(ng)
	defer db.Close()
}
