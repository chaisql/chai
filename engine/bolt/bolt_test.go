package bolt

import (
	"io/ioutil"
	"os"
	"path"

	bolt "github.com/etcd-io/bbolt"
	"github.com/stretchr/testify/require"
)

func tempDir(t require.TestingT) (string, func()) {
	dir, err := ioutil.TempDir("", "genji")
	require.NoError(t, err)

	return dir, func() {
		os.RemoveAll(dir)
	}
}

func tempDB(t require.TestingT) (*bolt.DB, func()) {
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

func tempBucket(t require.TestingT, writable bool) (*bolt.Bucket, func()) {
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

func countItems(t require.TestingT, b *bolt.Bucket) int {
	i := 0
	err := b.ForEach(func(k, v []byte) error {
		i++
		return nil
	})
	require.NoError(t, err)

	return i
}
