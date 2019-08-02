package memory_test

import (
	"testing"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/engine/enginetest"
	"github.com/asdine/genji/engine/memory"
	"github.com/stretchr/testify/require"
)

func TestMemoryEngine(t *testing.T) {
	enginetest.TestSuite(t, func() (engine.Engine, func()) {
		ng := memory.NewEngine()
		return ng, func() { ng.Close() }
	})
}

// func BenchmarkMemoryEngineTableInsert(b *testing.B) {
// 	tabletest.BenchmarkTableInsert(b, storeBuilder(b))
// }

// func BenchmarkMemoryEngineTableScan(b *testing.B) {
// 	tabletest.BenchmarkTableScan(b, storeBuilder(b))
// }

// func BenchmarkMemoryEngineIndexSet(b *testing.B) {
// 	indextest.BenchmarkIndexSet(b, indexBuilder(b))
// }

// func BenchmarkMemoryEngineIndexIteration(b *testing.B) {
// 	indextest.BenchmarkIndexIteration(b, indexBuilder(b))
// }

// func BenchmarkMemoryEngineIndexSeek(b *testing.B) {
// 	indextest.BenchmarkIndexSeek(b, indexBuilder(b))
// }

func storeBuilder(t require.TestingT) func() (engine.Store, func()) {
	return func() (engine.Store, func()) {
		ng := memory.NewEngine()
		tx, err := ng.Begin(true)
		require.NoError(t, err)

		err = tx.CreateStore("test")
		require.NoError(t, err)

		st, err := tx.Store("test")
		require.NoError(t, err)

		return st, func() {
			tx.Rollback()
			ng.Close()
		}
	}
}
