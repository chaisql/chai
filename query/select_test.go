package query

import (
	"testing"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

func TestSelectStatement(t *testing.T) {
	t.Run("NoIndex", func(t *testing.T) {
		tx, cleanup := createTable(t, 10, false)
		defer cleanup()

		res := Select().From(Table("test")).Where(IntField("age").Gt(20)).Limit(5).Offset(1).Exec(tx)
		require.NoError(t, res.Err())

		count, err := res.Count()
		require.NoError(t, err)
		require.Equal(t, 5, count)

		err = res.Iterate(func(recordID []byte, r record.Record) error {
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

		res := Select(Field("id"), Field("name")).From(Table("test")).Where(IntField("age").Gt(20)).Limit(5).Offset(1).Exec(tx)
		require.NoError(t, res.Err())

		count, err := res.Count()
		require.NoError(t, err)
		require.Equal(t, 5, count)

		err = res.Iterate(func(recordID []byte, r record.Record) error {
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

		res := Select().From(Table("test")).Where(StringField("name").Gt("john")).Limit(5).Offset(1).Exec(tx)
		require.NoError(t, res.Err())

		count, err := res.Count()
		require.NoError(t, err)
		require.Equal(t, 5, count)

		err = res.Iterate(func(recordID []byte, r record.Record) error {
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
		db, err := genji.New(memory.NewEngine())
		require.NoError(t, err)

		var n int
		err = db.Update(func(tx *genji.Tx) error {
			tb, err := tx.CreateTable("test")
			if err != nil {
				return err
			}

			_, err = tb.CreateIndex("a", index.Options{})
			if err != nil {
				return err
			}

			res := Select().From(tb).Where(And(StringField("a").Eq("foo"))).Limit(1).Run(tx)
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
