package bolt

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/coreos/bbolt"
	"github.com/stretchr/testify/require"
)

func tempDir(t *testing.T) (string, func()) {
	t.Helper()

	dir, err := ioutil.TempDir("", "genji")
	require.NoError(t, err)

	return dir, func() {
		os.RemoveAll(dir)
	}
}

func tempDB(t *testing.T) (*bolt.DB, func()) {
	t.Helper()

	dir, cleanup := tempDir(t)
	db, err := bolt.Open(path.Join(dir, "test.db"), 0660, nil)
	if err != nil {
		cleanup()
		require.NoError(t, err)
	}

	return db, func() {
		db.Close()
		cleanup()
	}
}

func tempBucket(t *testing.T, writable bool) (*bolt.Bucket, func()) {
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
