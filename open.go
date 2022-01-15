//go:build !wasm
// +build !wasm

package genji

import (
	"context"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji/engine/badgerengine"
)

// Open creates a Genji database at the given path.
// If path is equal to ":memory:" it will open an in-memory database,
// otherwise it will create an on-disk database using the BoltDB engine.
func Open(path string) (*DB, error) {
	var inMemory bool

	if path == ":memory:" {
		inMemory = true
		path = ""
	}

	opts := badger.DefaultOptions(path).WithLogger(nil).WithInMemory(inMemory)

	ng, err := badgerengine.NewEngine(opts)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	return New(ctx, ng)
}
