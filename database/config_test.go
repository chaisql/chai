package database

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine/memoryengine"
	"github.com/stretchr/testify/require"
)

func TestTableInfoStore(t *testing.T) {
	ng := memoryengine.NewEngine()
	defer ng.Close()

	tx, err := ng.Begin(true)
	require.NoError(t, err)
	defer tx.Rollback()

	err = tx.CreateStore([]byte("foo"))
	require.NoError(t, err)
	st, err := tx.GetStore([]byte("foo"))
	require.NoError(t, err)

	tcs := tableInfoStore{st}

	cfg := TableConfig{
		FieldConstraints: []FieldConstraint{
			{Path: []string{"k"}, Type: document.Float64Value, IsPrimaryKey: true},
		},
	}

	// Inserting one tableInfo should work.
	ti, err := tcs.Insert("foo1", cfg)
	require.NoError(t, err)

	// Inserting an existing tableInfo should not work.
	_, err = tcs.Insert("foo1", cfg)
	require.Equal(t, err, ErrTableAlreadyExists)

	// Listing all tables should return their name
	// lexicographically ordered.
	_, _ = tcs.Insert("foo3", cfg)
	_, _ = tcs.Insert("foo2", cfg)
	lt, err := tcs.ListTables()
	require.NoError(t, err)
	require.Equal(t, []string{"foo1", "foo2", "foo3"}, lt)

	// Getting an existing tableInfo should work.
	received, err := tcs.Get("foo1")
	require.NoError(t, err)
	require.Equal(t, ti, received)

	// Getting a non-existing tableInfo should not work.
	_, err = tcs.Get("unknown")
	require.Equal(t, ErrTableNotFound, err)

	// Updating the config table.
	fc := FieldConstraint{Path: []string{"j"}, Type: document.TextValue, IsNotNull: true}
	cfg.FieldConstraints = append(cfg.FieldConstraints, fc)
	err = tcs.Replace("foo1", &cfg)
	require.NoError(t, err)

	received, err = tcs.Get("foo1")
	require.NoError(t, err)
	require.Equal(t, ti.storeID, received.storeID)
	require.Equal(t, cfg.FieldConstraints, received.FieldConstraints)

	// Deleting an existing tableInfo should work.
	err = tcs.Delete("foo1")
	require.NoError(t, err)

	// Deleting a non-existing tableInfo should not work.
	err = tcs.Delete("foo1")
	require.Equal(t, ErrTableNotFound, err)
}

func TestIndexStore(t *testing.T) {
	ng := memoryengine.NewEngine()
	defer ng.Close()

	tx, err := ng.Begin(true)
	require.NoError(t, err)
	defer tx.Rollback()

	err = tx.CreateStore([]byte("test"))
	require.NoError(t, err)
	st, err := tx.GetStore([]byte("test"))
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
