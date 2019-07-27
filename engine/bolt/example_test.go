package bolt_test

import (
	"io/ioutil"
	"log"
	"os"
	"path"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/bolt"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
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

	err = db.Update(func(tx *genji.Tx) error {
		err = tx.CreateTable("Table")
		if err != nil {
			return err
		}

		t, err := tx.Table("Table")
		if err != nil {
			return err
		}

		r := record.FieldBuffer{
			field.NewString("Name", "foo"),
			field.NewInt("Age", 10),
		}

		_, err = t.Insert(r)
		return err
	})
	if err != nil {
		log.Fatal(err)
	}
}
