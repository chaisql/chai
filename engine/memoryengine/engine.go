package memoryengine

import (
	"github.com/dgraph-io/badger/v2"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/engine/badgerengine"
)

// NewEngine creates a badger engine which stores data in memory.
func NewEngine() engine.Engine {
	opts := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
	ng, err := badgerengine.NewEngine(opts)
	if err != nil {
		panic(err)
	}

	return ng
}
