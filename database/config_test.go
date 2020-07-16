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

	db, err := New(ng)
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.Begin(true)
	require.NoError(t, err)
	defer tx.Rollback()

	info := &TableInfo{
		FieldConstraints: []FieldConstraint{
			{Path: []string{"k"}, Type: document.DoubleValue, IsPrimaryKey: true},
		},
	}

	// Inserting one TableInfo should work.
	err = tx.tableInfoStore.Insert(tx.Tx, "foo1", info)
	require.NoError(t, err)

	// Inserting an existing TableInfo should not work.
	err = tx.tableInfoStore.Insert(tx.Tx, "foo1", info)
	require.Equal(t, err, ErrTableAlreadyExists)

	// Listing all tables should return their name
	// lexicographically ordered.
	_ = tx.tableInfoStore.Insert(tx.Tx, "foo3", info)
	_ = tx.tableInfoStore.Insert(tx.Tx, "foo2", info)
	lt := tx.tableInfoStore.ListTables()
	require.Equal(t, []string{"foo1", "foo2", "foo3"}, lt)

	// Getting an existing TableInfo should work.
	received, err := tx.tableInfoStore.Get("foo1")
	require.NoError(t, err)
	require.NotNil(t, received.storeID)

	// Getting a non-existing TableInfo should not work.
	_, err = tx.tableInfoStore.Get("unknown")
	require.Equal(t, ErrTableNotFound, err)

	// Deleting an existing TableInfo should work.
	err = tx.tableInfoStore.Delete(tx.Tx, "foo1")
	require.NoError(t, err)

	// Deleting a non-existing TableInfo should not work.
	err = tx.tableInfoStore.Delete(tx.Tx, "foo1")
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

		// Updating the index should work
		cfg.Unique = false
		err = idxs.Replace(cfg.IndexName, cfg)
		require.NoError(t, err)
		idxcfg, err = idxs.Get("idx_test")
		require.NoError(t, err)
		require.False(t, idxcfg.Unique)

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
