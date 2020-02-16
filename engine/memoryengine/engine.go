package memoryengine

import (
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/engine/badgerengine"
	"github.com/dgraph-io/badger/v2"
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
