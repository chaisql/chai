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
	dir, cleanup := tempDir(t)
	ng, err := bolt.NewEngine(path.Join(dir, "test.db"), 0600, nil)
	require.NoError(t, err)

	indextest.TestSuite(t, func() (index.Index, func()) {
		tx, err := ng.Begin(true)
		require.NoError(t, err)

		_, err = tx.CreateTable("test")
		require.NoError(t, err)

		idx, err := tx.CreateIndex("test", "idx")
		require.NoError(t, err)

		return idx, func() {
			tx.Rollback()
		}
	})

	ng.Close()
	cleanup()
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

func tableBuilder(t require.TestingT) (func() (table.Table, func()), func()) {
	dir, cleanup := tempDir(t)
	ng, err := bolt.NewEngine(path.Join(dir, "test.db"), 0600, nil)
	require.NoError(t, err)

	return func() (table.Table, func()) {
			tx, err := ng.Begin(true)
			require.NoError(t, err)

			tb, err := tx.CreateTable("test")
			require.NoError(t, err)

			return tb, func() {
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
