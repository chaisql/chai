package query_test

import (
	"testing"

	"github.com/asdine/genji/query"
	"github.com/stretchr/testify/require"
)

func TestCreateTableStatement(t *testing.T) {
	tx, cleanup := createTable(t, 0, false)
	defer cleanup()

	res := query.CreateTable("foo").Exec(tx)
	require.NoError(t, res.Err())

	_, err := tx.GetTable("foo")
	require.NoError(t, err)

	res = query.CreateTable("foo").Exec(tx)
	require.Error(t, res.Err())

	res = query.CreateTable("foo").IfNotExists().Exec(tx)
	require.NoError(t, res.Err())

	res = query.CreateTable("bar").IfNotExists().Exec(tx)
	require.NoError(t, res.Err())
}

func TestCreateIndexStatement(t *testing.T) {
	tx, cleanup := createTable(t, 0, false)
	defer cleanup()

	res := query.CreateIndex("foo", "test", "a").Exec(tx)
	require.NoError(t, res.Err())

	res = query.CreateIndex("foo", "test", "a").Exec(tx)
	require.Error(t, res.Err())

	res = query.CreateIndex("foo", "test", "a").IfNotExists().Exec(tx)
	require.NoError(t, res.Err())

	res = query.CreateIndex("bar", "test", "a").IfNotExists().Exec(tx)
	require.NoError(t, res.Err())
}
