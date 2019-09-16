package query

import (
	"errors"
	"fmt"
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
		res, err = dbx.Exec(fmt.Sprintf("INSERT INTO test (a, b, c) VALUES (%d, %d, %d)", i+1, i+2, i+3))
		require.NoError(t, err)
		n, err = res.RowsAffected()
		require.NoError(t, err)
		require.EqualValues(t, 1, n)
		time.Sleep(time.Millisecond) // ensure records are stored in order
	}

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
}
