package bolt_test

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/engine/bolt"
	"github.com/asdine/genji/engine/enginetest"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/index/indextest"
	"github.com/asdine/genji/table"
	"github.com/asdine/genji/table/tabletest"
	"github.com/stretchr/testify/require"
)

func TestBoltEngine(t *testing.T) {
	enginetest.TestSuite(t, func() (engine.Engine, func()) {
		dir, cleanup := tempDir(t)
		ng, err := bolt.NewEngine(path.Join(dir, "test.db"), 0600, nil)
		require.NoError(t, err)
		return ng, cleanup
	})
}

func TestBoltEngineIndex(t *testing.T) {
	builder, cleanup := indexBuilder(t)
	defer cleanup()

	indextest.TestSuite(t, builder)
}

func TestBoltEngineTable(t *testing.T) {
	builder, cleanup := tableBuilder(t)
	defer cleanup()

	tabletest.TestSuite(t, builder)
}

func BenchmarkBoltEngineTableInsert(b *testing.B) {
	builder, cleanup := tableBuilder(b)
	defer cleanup()

	tabletest.BenchmarkTableInsert(b, builder)
}

func BenchmarkBoltEngineTableScan(b *testing.B) {
	builder, cleanup := tableBuilder(b)
	defer cleanup()

	tabletest.BenchmarkTableScan(b, builder)
}

func BenchmarkBoltEngineIndexSet(b *testing.B) {
	builder, cleanup := indexBuilder(b)
	defer cleanup()

	indextest.BenchmarkIndexSet(b, builder)
}

func BenchmarkBoltEngineIndexIteration(b *testing.B) {
	builder, cleanup := indexBuilder(b)
	defer cleanup()

	indextest.BenchmarkIndexIteration(b, builder)
}

func BenchmarkBoltEngineIndexSeek(b *testing.B) {
	builder, cleanup := indexBuilder(b)
	defer cleanup()

	indextest.BenchmarkIndexSeek(b, builder)
}

func tableBuilder(t require.TestingT) (func() (table.Table, func()), func()) {
	dir, cleanup := tempDir(t)
	ng, err := bolt.NewEngine(path.Join(dir, "test.db"), 0600, nil)
	require.NoError(t, err)

	return func() (table.Table, func()) {
			tx, err := ng.Begin(true)
			require.NoError(t, err)

			err = tx.CreateTable("test")
			require.NoError(t, err)

			tb, err := tx.Table("test")
			require.NoError(t, err)

			return tb, func() {
				tx.Rollback()
			}
		}, func() {
			ng.Close()
			cleanup()
		}
}

func indexBuilder(t require.TestingT) (func() (index.Index, func()), func()) {
	dir, cleanup := tempDir(t)
	ng, err := bolt.NewEngine(path.Join(dir, "test.db"), 0600, nil)
	require.NoError(t, err)

	return func() (index.Index, func()) {
			tx, err := ng.Begin(true)
			require.NoError(t, err)

			err = tx.CreateTable("test")
			require.NoError(t, err)

			idx, err := tx.CreateIndex("test", "idx")
			require.NoError(t, err)

			return idx, func() {
				tx.Rollback()
			}
		}, func() {
			ng.Close()
			cleanup()
		}
}

func tempDir(t require.TestingT) (string, func()) {
	dir, err := ioutil.TempDir("", "genji")
	require.NoError(t, err)

	return dir, func() {
		os.RemoveAll(dir)
	}
}
