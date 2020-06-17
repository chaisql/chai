package database

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine/memoryengine"
	"github.com/stretchr/testify/require"
)

func TestTableConfigStore(t *testing.T) {
	ng := memoryengine.NewEngine()
	defer ng.Close()

	tx, err := ng.Begin(true)
	require.NoError(t, err)
	defer tx.Rollback()

	err = tx.CreateStore("foo")
	require.NoError(t, err)
	st, err := tx.GetStore("foo")
	require.NoError(t, err)

	tcs := tableConfigStore{st}

	cfg := TableConfig{
		FieldConstraints: []FieldConstraint{
			{Path: []string{"k"}, Type: document.Float64Value, IsPrimaryKey: true},
		},
		LastKey: 100,
	}

	// inserting one should work
	err = tcs.Insert("foo-table", cfg)
	require.NoError(t, err)

	// inserting one with the same name should not work
	err = tcs.Insert("foo-table", cfg)
	require.Equal(t, err, ErrTableAlreadyExists)

	// getting an existing table should work
	received, err := tcs.Get("foo-table")
	require.NoError(t, err)
	require.Equal(t, cfg, *received)

	// getting a non-existing table should not work
	_, err = tcs.Get("unknown")
	require.Equal(t, ErrTableNotFound, err)

	// deleting an existing table should work
	err = tcs.Delete("foo-table")
	require.NoError(t, err)

	// deleting a non-existing table should not work
	err = tcs.Delete("foo-table")
	require.Equal(t, ErrTableNotFound, err)
}

func TestIndexStore(t *testing.T) {
	ng := memoryengine.NewEngine()
	defer ng.Close()

	tx, err := ng.Begin(true)
	require.NoError(t, err)
	defer tx.Rollback()

	err = tx.CreateStore("test")
	require.NoError(t, err)
	st, err := tx.GetStore("test")
	require.NoError(t, err)

	idxs := indexStore{st}

	t.Run("Basic operations", func(t *testing.T) {
		cfg := IndexConfig{
			TableName: "test",
			IndexName: "idx_test",
			Unique:    true,
		}

		err = idxs.Insert(cfg)
		require.NoError(t, err)

		// Inserting the same index should fail.
		err = idxs.Insert(cfg)
		require.EqualError(t, err, ErrIndexAlreadyExists.Error())

		idxcfg, err := idxs.Get("idx_test")
		require.NoError(t, err)
		require.Equal(t, &cfg, idxcfg)

		err = idxs.Delete("idx_test")
		require.NoError(t, err)

		// Getting a non existing index should fail.
		_, err = idxs.Get("idx_test")
		require.EqualError(t, err, ErrIndexNotFound.Error())
	})

	t.Run("List all indexes", func(t *testing.T) {
		idxcfgs := []*IndexConfig{
			{TableName: "test1", IndexName: "idx_test1", Unique: true},
			{TableName: "test2", IndexName: "idx_test2", Unique: true},
			{TableName: "test3", IndexName: "idx_test3", Unique: true},
		}
		for _, v := range idxcfgs {
			err = idxs.Insert(*v)
			require.NoError(t, err)
		}

		list, err := idxs.ListAll()
		require.NoError(t, err)
		require.Len(t, list, len(idxcfgs))
		require.EqualValues(t, idxcfgs, list)

		// Removing one index should remove only one index.
		err = idxs.Delete("idx_test1")
		require.NoError(t, err)

		list, err = idxs.ListAll()
		require.NoError(t, err)
		require.Len(t, list, len(idxcfgs)-1)
	})
}
