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
	bbolt "github.com/etcd-io/bbolt"
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
	indextest.TestSuite(t, func() (index.Index, func()) {
		dir, cleanup := tempDir(t)
		ng, err := bolt.NewEngine(path.Join(dir, "test.db"), 0600, nil)
		require.NoError(t, err)

		tx, err := ng.Begin(true)
		require.NoError(t, err)

		_, err = tx.CreateTable("test")
		require.NoError(t, err)

		idx, err := tx.CreateIndex("test", "idx")
		require.NoError(t, err)

		return idx, func() {
			tx.Rollback()
			ng.Close()
			cleanup()
		}
	})
}

func TestBoltEngineTable(t *testing.T) {
	tabletest.TestSuite(t, func() (table.Table, func()) {
		dir, cleanup := tempDir(t)
		ng, err := bolt.NewEngine(path.Join(dir, "test.db"), 0600, nil)
		require.NoError(t, err)

		tx, err := ng.Begin(true)
		require.NoError(t, err)

		tb, err := tx.CreateTable("test")
		require.NoError(t, err)

		return tb, func() {
			tx.Rollback()
			ng.Close()
			cleanup()
		}
	})
}

func tempDir(t require.TestingT) (string, func()) {
	dir, err := ioutil.TempDir("", "genji")
	require.NoError(t, err)

	return dir, func() {
		os.RemoveAll(dir)
	}
}

func tempDB(t require.TestingT) (*bbolt.DB, func()) {
	dir, cleanup := tempDir(t)
	db, err := bbolt.Open(path.Join(dir, "test.db"), 0660, nil)
	if err != nil {
		cleanup()
		require.NoError(t, err)
	}

	return db, func() {
		db.Close()
		cleanup()
	}
}

func tempBucket(t require.TestingT, writable bool) (*bbolt.Bucket, func()) {
	db, cleanup := tempDB(t)

	tx, err := db.Begin(writable)
	if err != nil {
		cleanup()
		require.NoError(t, err)
	}

	b, err := tx.CreateBucketIfNotExists([]byte("test"))
	if err != nil {
		cleanup()
		require.NoError(t, err)
	}

	return b, func() {
		tx.Rollback()
		cleanup()
	}
}

func countItems(t require.TestingT, b *bbolt.Bucket) int {
	i := 0
	err := b.ForEach(func(k, v []byte) error {
		i++
		return nil
	})
	require.NoError(t, err)

	return i
}
