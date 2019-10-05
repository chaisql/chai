package query_test

import (
	"testing"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/query"
	"github.com/asdine/genji/query/expr"
	"github.com/asdine/genji/query/q"
	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

func TestSelectStatement(t *testing.T) {
	t.Run("NoIndex", func(t *testing.T) {
		tx, cleanup := createTable(t, 10, false)
		defer cleanup()

		res := query.Select().From(q.Table("test")).Where(q.IntField("age").Gt(20)).Limit(5).Offset(1).Exec(tx)
		require.NoError(t, res.Err())

		count, err := res.Count()
		require.NoError(t, err)
		require.Equal(t, 5, count)

		err = res.Iterate(func(r record.Record) error {
			_, err := r.GetField("id")
			require.NoError(t, err)
			_, err = r.GetField("name")
			require.NoError(t, err)
			_, err = r.GetField("age")
			require.NoError(t, err)
			_, err = r.GetField("group")
			require.NoError(t, err)

			return nil
		})
		require.NoError(t, err)
	})

	t.Run("WithFields", func(t *testing.T) {
		tx, cleanup := createTable(t, 10, false)
		defer cleanup()

		res := query.Select(q.Field("id"), q.Field("name")).From(q.Table("test")).Where(q.IntField("age").Gt(20)).Limit(5).Offset(1).Exec(tx)
		require.NoError(t, res.Err())

		count, err := res.Count()
		require.NoError(t, err)
		require.Equal(t, 5, count)

		err = res.Iterate(func(r record.Record) error {
			_, err := r.GetField("id")
			require.NoError(t, err)
			_, err = r.GetField("name")
			require.NoError(t, err)
			_, err = r.GetField("age")
			require.Error(t, err)
			_, err = r.GetField("group")
			require.Error(t, err)

			return nil
		})
		require.NoError(t, err)
	})

	t.Run("WithIndex", func(t *testing.T) {
		tx, cleanup := createTable(t, 10, true)
		defer cleanup()

		res := query.Select().From(q.Table("test")).Where(q.StringField("name").Gt("john")).Limit(5).Offset(1).Exec(tx)
		require.NoError(t, res.Err())

		count, err := res.Count()
		require.NoError(t, err)
		require.Equal(t, 5, count)

		err = res.Iterate(func(r record.Record) error {
			_, err := r.GetField("id")
			require.NoError(t, err)
			_, err = r.GetField("name")
			require.NoError(t, err)
			_, err = r.GetField("age")
			require.NoError(t, err)
			_, err = r.GetField("group")
			require.NoError(t, err)

			return nil
		})
		require.NoError(t, err)
	})

	t.Run("WithEmptyIndex", func(t *testing.T) {
		db, err := database.New(memory.NewEngine())
		require.NoError(t, err)

		var n int
		err = db.Update(func(tx *database.Tx) error {
			tb, err := tx.CreateTable("test")
			if err != nil {
				return err
			}

			_, err = tb.CreateIndex("idx_test_a", "a", index.Options{})
			if err != nil {
				return err
			}

			res := query.Select().From(tb).Where(expr.And(q.StringField("a").Eq("foo"), q.StringField("a").Eq("foo"))).Limit(1).Exec(tx)
			if res.Err() != nil {
				return res.Err()
			}

			n, err = res.Count()
			return err
		})
		require.NoError(t, err)
		require.Equal(t, 0, n)
	})
}
