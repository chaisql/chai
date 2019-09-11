package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateTableStatement(t *testing.T) {
	tx, cleanup := createTable(t, 0, false)
	defer cleanup()

	res := CreateTable("foo").Exec(tx)
	require.NoError(t, res.Err())

	_, err := tx.GetTable("foo")
	require.NoError(t, err)

	res = CreateTable("foo").Exec(tx)
	require.Error(t, res.Err())

	res = CreateTable("foo").IfNotExists().Exec(tx)
	require.NoError(t, res.Err())

	res = CreateTable("bar").IfNotExists().Exec(tx)
	require.NoError(t, res.Err())
}
