package query_test

import (
	"testing"

	"github.com/asdine/genji/query"
	"github.com/asdine/genji/query/expr"
	"github.com/asdine/genji/query/q"
	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

func TestInsertStatement(t *testing.T) {
	t.Run("NoFields", func(t *testing.T) {
		tx, cleanup := createTable(t, 10, false)
		defer cleanup()

		res := query.Insert().Into(q.Table("test")).Values(expr.IntValue(5), expr.StringValue("hello"), expr.IntValue(50), expr.IntValue(5)).Exec(tx)
		require.Error(t, res.Err())
	})

	t.Run("WithFields", func(t *testing.T) {
		tx, cleanup := createTable(t, 10, false)
		defer cleanup()

		res := query.Insert().Into(q.Table("test")).Fields("a", "b").Values(expr.IntValue(5), expr.StringValue("hello")).Exec(tx)
		require.NoError(t, res.Err())

		tb, err := tx.GetTable("test")
		require.NoError(t, err)

		st := record.NewStream(tb)
		count, err := st.Count()
		require.NoError(t, err)
		require.Equal(t, 11, count)
	})
}
