package engineutil

import (
	"fmt"

	"github.com/asdine/genji/engine"
)

func DumpEngine(e engine.Engine) error {
	tx, err := e.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stores, err := tx.ListStores("")
	if err != nil {
		return err
	}

	for _, name := range stores {
		s, err := tx.GetStore(name)
		if err != nil {
			return err
		}
		fmt.Println("--------------")
		fmt.Printf("Store: %s\n", name)
		fmt.Println("--------------")
		err = DumpStore(s)
		if err != nil {
			return err
		}
		fmt.Println("--------------")

	}
	return nil
}

func DumpStore(s engine.Store) error {
	it := s.NewIterator(engine.IteratorConfig{})

	for it.Seek(nil); it.Valid(); it.Next() {
		item := it.Item()
		key := item.Key()
		value, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		fmt.Printf("Key\t: %v\n", key)
		fmt.Printf("Value\t: %v\n\n", value)
	}

	return nil
}
