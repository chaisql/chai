package query

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/record"
	"github.com/stretchr/testify/require"
)

type rectest struct {
	a, b, c int
}

func (rt *rectest) Scan(src interface{}) error {
	r, ok := src.(record.Record)
	if !ok {
		return errors.New("unable to scan returned data")
	}

	return rt.ScanRecord(r)
}

func (rt *rectest) ScanRecord(r record.Record) error {
	f, err := r.GetField("a")
	if err != nil {
		return err
	}
	v, err := f.Decode()
	if err != nil {
		return err
	}

	rt.a = int(v.(int64))

	f, err = r.GetField("b")
	if err != nil {
		return err
	}
	v, err = f.Decode()
	if err != nil {
		return err
	}

	rt.b = int(v.(int64))

	f, err = r.GetField("c")
	if err != nil {
		return err
	}
	v, err = f.Decode()
	if err != nil {
		return err
	}

	rt.c = int(v.(int64))
	return nil
}

func TestDriver(t *testing.T) {
	db, err := genji.New(memory.NewEngine())
	require.NoError(t, err)
	defer db.Close()

	dbx := NewSQLDB(db)

	res, err := dbx.Exec("CREATE TABLE test")
	require.NoError(t, err)
	n, err := res.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 0, n)

	for i := 0; i < 10; i++ {
		res, err = dbx.Exec("INSERT INTO test (a, b, c) VALUES (?, ?, ?)", i+1, i+2, i+3)
		require.NoError(t, err)
		n, err = res.RowsAffected()
		require.NoError(t, err)
		require.EqualValues(t, 1, n)
		time.Sleep(time.Millisecond) // ensure records are stored in order
	}

	t.Run("Wildcard", func(t *testing.T) {
		rows, err := dbx.Query("SELECT * FROM test")
		require.NoError(t, err)
		defer rows.Close()

		var count int
		var rt rectest
		for rows.Next() {
			err = rows.Scan(&rt)
			require.NoError(t, err)
			require.Equal(t, rectest{count + 1, count + 2, count + 3}, rt)
			count++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 10, count)
	})

	t.Run("Multiple fields", func(t *testing.T) {
		rows, err := dbx.Query("SELECT a, c FROM test")
		require.NoError(t, err)
		defer rows.Close()

		var count int
		var a, c int
		for rows.Next() {
			err = rows.Scan(&a, &c)
			require.NoError(t, err)
			require.Equal(t, count+1, a)
			require.Equal(t, count+3, c)
			count++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 10, count)
	})

	t.Run("Params", func(t *testing.T) {
		rows, err := dbx.Query("SELECT a FROM test WHERE a = ? AND b = ?", 5, 6)
		require.NoError(t, err)
		defer rows.Close()

		var count int
		var a int
		for rows.Next() {
			err = rows.Scan(&a)
			require.NoError(t, err)
			require.Equal(t, 5, a)
			count++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 1, count)
	})

	t.Run("Named Params", func(t *testing.T) {
		rows, err := dbx.Query("SELECT a FROM test WHERE a = $val", sql.Named("val", 5))
		require.NoError(t, err)
		defer rows.Close()

		var count int
		var a int
		for rows.Next() {
			err = rows.Scan(&a)
			require.NoError(t, err)
			require.Equal(t, 5, a)
			count++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 1, count)
	})
}
