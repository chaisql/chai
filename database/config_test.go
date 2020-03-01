package database

import (
	"testing"

	"github.com/asdine/genji/document"
	"github.com/asdine/genji/engine/memoryengine"
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
