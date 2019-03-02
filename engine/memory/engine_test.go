package memory_test

import (
	"testing"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/engine/enginetest"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/index/indextest"
	"github.com/asdine/genji/table"
	"github.com/asdine/genji/table/tabletest"
	"github.com/stretchr/testify/require"
)

func TestMemoryEngine(t *testing.T) {
	enginetest.TestSuite(t, func() (engine.Engine, func()) {
		ng := memory.NewEngine()
		return ng, func() { ng.Close() }
	})
}

func TestMemoryEngineIndex(t *testing.T) {
	indextest.TestSuite(t, indexBuilder(t))
}

func TestMemoryEngineTable(t *testing.T) {
	tabletest.TestSuite(t, tableBuilder(t))
}

func BenchmarkMemoryEngineTableInsert(b *testing.B) {
	tabletest.BenchmarkTableInsert(b, tableBuilder(b))
}

func BenchmarkMemoryEngineTableScan(b *testing.B) {
	tabletest.BenchmarkTableScan(b, tableBuilder(b))
}

func BenchmarkMemoryEngineIndexSet(b *testing.B) {
	indextest.BenchmarkIndexSet(b, indexBuilder(b))
}

func BenchmarkMemoryEngineIndexIteration(b *testing.B) {
	indextest.BenchmarkIndexIteration(b, indexBuilder(b))
}

func BenchmarkMemoryEngineIndexSeek(b *testing.B) {
	indextest.BenchmarkIndexSeek(b, indexBuilder(b))
}

func tableBuilder(t require.TestingT) func() (table.Table, func()) {
	return func() (table.Table, func()) {
		ng := memory.NewEngine()
		tx, err := ng.Begin(true)
		require.NoError(t, err)

		tb, err := tx.CreateTable("test")
		require.NoError(t, err)

		return tb, func() {
			tx.Rollback()
			ng.Close()
		}
	}
}

func indexBuilder(t require.TestingT) func() (index.Index, func()) {
	return func() (index.Index, func()) {
		ng := memory.NewEngine()
		tx, err := ng.Begin(true)
		require.NoError(t, err)

		_, err = tx.CreateTable("test")
		require.NoError(t, err)

		idx, err := tx.CreateIndex("test", "idx")
		require.NoError(t, err)

		return idx, func() {
			tx.Rollback()
			ng.Close()
		}
	}
}
