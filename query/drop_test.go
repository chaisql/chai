package query_test

import (
	"testing"

	"github.com/asdine/genji/query"
	"github.com/stretchr/testify/require"
)

func TestDropTableStatement(t *testing.T) {
	tx, cleanup := createTable(t, 0, false)
	defer cleanup()

	res := query.DropTable("").Exec(tx)
	require.Error(t, res.Err())

	res = query.CreateTable("foo").Exec(tx)
	require.NoError(t, res.Err())

	res = query.DropTable("foo").Exec(tx)
	require.NoError(t, res.Err())

	res = query.DropTable("foo").Exec(tx)
	require.Error(t, res.Err())

	res = query.DropTable("foo").IfExists().Exec(tx)
	require.NoError(t, res.Err())

	res = query.CreateTable("foo").Exec(tx)
	require.NoError(t, res.Err())

	res = query.DropTable("foo").IfExists().Exec(tx)
	require.NoError(t, res.Err())

	_, err := tx.GetTable("foo")
	require.Error(t, err)
}

func TestDropIndexStatement(t *testing.T) {
	tx, cleanup := createTable(t, 0, false)
	defer cleanup()

	res := query.CreateIndex("foo").On("test").Field("a").Exec(tx)
	require.NoError(t, res.Err())

	res = query.DropIndex("foo").Exec(tx)
	require.NoError(t, res.Err())

	res = query.DropIndex("foo").Exec(tx)
	require.Error(t, res.Err())

	res = query.DropIndex("foo").IfExists().Exec(tx)
	require.NoError(t, res.Err())
}
