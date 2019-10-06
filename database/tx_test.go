package database_test

import (
	"testing"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/index"
	"github.com/stretchr/testify/require"
)

func TestTxCreateIndex(t *testing.T) {
	t.Run("Should create an index and return it", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		idx, err := tx.CreateIndex("idxFoo", "test", "foo", index.Options{})
		require.NoError(t, err)
		require.NotNil(t, idx)
	})

	t.Run("Should fail if it already exists", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		_, err := tx.CreateIndex("idxFoo", "test", "foo", index.Options{})
		require.NoError(t, err)

		_, err = tx.CreateIndex("idxFoo", "test", "foo", index.Options{})
		require.Equal(t, database.ErrIndexAlreadyExists, err)
	})
}

func TestTxCreateIndexIfNotExists(t *testing.T) {
	t.Run("Should create an index and return it", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		idx, err := tx.CreateIndexIfNotExists("idxFoo", "test", "foo", index.Options{})
		require.NoError(t, err)
		require.NotNil(t, idx)
	})

	t.Run("Should succeed if it already exists", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		idx, err := tx.CreateIndex("idxFoo", "test", "foo", index.Options{})
		require.NoError(t, err)

		idx2, err := tx.CreateIndexIfNotExists("idxFoo", "test", "foo", index.Options{})
		require.NoError(t, err)
		require.Equal(t, idx, idx2)
	})
}

func TestTxDropIndex(t *testing.T) {
	t.Run("Should drop an index", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		_, err := tx.CreateIndex("idxFoo", "test", "foo", index.Options{})
		require.NoError(t, err)

		err = tx.DropIndex("idxFoo")
		require.NoError(t, err)

		_, err = tx.GetIndex("idxFoo")
		require.Error(t, err)
	})

	t.Run("Should fail if it doesn't exist", func(t *testing.T) {
		tx, cleanup := newTestDB(t)
		defer cleanup()

		err := tx.DropIndex("idxFoo")
		require.Equal(t, database.ErrIndexNotFound, err)
	})
}
