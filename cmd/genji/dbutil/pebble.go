package dbutil

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/genjidb/genji/internal/encoding"
)

type DumpPebbleOptions struct {
	KeysOnly bool
}

func DumpPebble(c context.Context, db *pebble.DB, opt DumpPebbleOptions) error {
	iter, err := db.NewIter(nil)
	if err != nil {
		return err
	}
	defer iter.Close()

	var curns int64
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		ns, _ := encoding.DecodeInt(k)
		if curns != 0 && ns != curns {
			fmt.Println()
		}
		curns = ns

		if opt.KeysOnly {
			fmt.Println(iter.Key())
		} else {
			fmt.Printf("%v: %v\n", iter.Key(), iter.Value())
		}
	}

	return nil
}
