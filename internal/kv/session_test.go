package kv_test

import (
	"bytes"
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/chaisql/chai"
	"github.com/chaisql/chai/internal/encoding"
	"github.com/chaisql/chai/internal/engine"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/stretchr/testify/require"
)

func getValue(t *testing.T, st engine.Session, key []byte) []byte {
	v, err := st.Get([]byte(key))
	require.NoError(t, err)
	return v
}

func TestReadOnly(t *testing.T) {
	t.Run("Read-Only write attempts", func(t *testing.T) {
		sro := testutil.NewEngine(t).NewSnapshotSession()
		defer sro.Close()

		tests := []struct {
			name string
			fn   func(*error)
		}{
			{"StorePut", func(err *error) { *err = sro.Put([]byte("id"), nil) }},
			{"StoreDelete", func(err *error) { *err = sro.Delete([]byte("id")) }},
			{"StoreDeleteRange", func(err *error) { *err = sro.DeleteRange([]byte("start"), []byte("end")) }},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				var err error
				test.fn(&err)

				require.Error(t, err)
			})
		}
	})
}

func kvBuilder(t testing.TB) engine.Session {
	ng := testutil.NewEngine(t)
	s := ng.NewBatchSession()

	t.Cleanup(func() {
		s.Close()
	})

	return s
}

func TestBatchCommit(t *testing.T) {
	ng := testutil.NewEngine(t)

	batch := ng.NewBatchSession()
	defer batch.Close()

	var k int64
	for i := int64(0); i < 10; i++ {
		for j := int64(0); j < 10; j++ {
			k++
			key := encoding.EncodeInt(encoding.EncodeInt(nil, 10), j)
			err := batch.Put(key, encoding.EncodeInt(nil, k))
			require.NoError(t, err)
		}
	}

	// snapshots created during the write transaction should not see the changes
	ss := ng.NewSnapshotSession()
	_, err := ss.Get(encoding.EncodeInt(encoding.EncodeInt(nil, 10), 9))
	require.Error(t, err)
	err = ss.Close()
	require.NoError(t, err)

	// commit the write transaction
	err = batch.Commit()
	require.NoError(t, err)

	// try to read again, should see the changes
	ss = ng.NewSnapshotSession()
	for i := int64(9); i >= 0; i-- {
		key := encoding.EncodeInt(encoding.EncodeInt(nil, 10), i)
		v, err := ss.Get(key)
		require.NoError(t, err)
		require.Equal(t, encoding.EncodeInt(nil, k), v)
		k--
	}
	err = ss.Close()
	require.NoError(t, err)
}

func TestRollback(t *testing.T) {
	ng := testutil.NewEngine(t)

	s := ng.NewBatchSession()
	defer s.Close()

	var k int64
	for i := int64(0); i < 10; i++ {
		for j := int64(0); j < 10; j++ {
			k++
			key := encoding.EncodeInt(encoding.EncodeInt(nil, 10), j)
			err := s.Put(key, encoding.EncodeInt(nil, k))
			require.NoError(t, err)
		}
	}

	err := s.Close()
	require.NoError(t, err)

	err = ng.Rollback()
	require.NoError(t, err)

	snapshot := ng.NewSnapshotSession()
	for i := int64(9); i >= 0; i-- {
		key := encoding.EncodeInt(encoding.EncodeInt(nil, 10), i)
		_, err = snapshot.Get(key)
		require.ErrorIs(t, err, engine.ErrKeyNotFound)
	}
}

func TestStorePut(t *testing.T) {
	key := encoding.EncodeInt64(nil, 1)

	t.Run("Should insert data", func(t *testing.T) {
		st := kvBuilder(t)

		err := st.Put(key, []byte("FOO"))
		require.NoError(t, err)

		v := getValue(t, st, key)
		require.Equal(t, []byte("FOO"), v)
	})

	t.Run("Should replace existing key", func(t *testing.T) {
		st := kvBuilder(t)

		err := st.Put(key, []byte("FOO"))
		require.NoError(t, err)

		err = st.Put(key, []byte("BAR"))
		require.NoError(t, err)

		v := getValue(t, st, key)
		require.Equal(t, []byte("BAR"), v)
	})

	t.Run("Should fail when key is nil or empty", func(t *testing.T) {
		st := kvBuilder(t)

		err := st.Put(nil, []byte("FOO"))
		require.Error(t, err)

		err = st.Put([]byte(""), []byte("BAR"))
		require.Error(t, err)
	})

	t.Run("Should fail when value is nil or empty", func(t *testing.T) {
		st := kvBuilder(t)

		err := st.Put(key, nil)
		require.Error(t, err)

		err = st.Put(key, []byte(""))
		require.Error(t, err)
	})
}

// TestStoreGet verifies Get behaviour.
func TestStoreGet(t *testing.T) {
	foo := encoding.EncodeInt64(nil, 1)
	bar := encoding.EncodeInt64(nil, 2)

	t.Run("Should fail if not found", func(t *testing.T) {
		st := kvBuilder(t)

		r, err := st.Get(foo)
		require.ErrorIs(t, err, engine.ErrKeyNotFound)
		require.Nil(t, r)
	})

	t.Run("Should return the right key", func(t *testing.T) {
		st := kvBuilder(t)

		err := st.Put(foo, []byte("FOO"))
		require.NoError(t, err)
		err = st.Put(bar, []byte("BAR"))
		require.NoError(t, err)

		v := getValue(t, st, foo)
		require.Equal(t, []byte("FOO"), v)

		v = getValue(t, st, bar)
		require.Equal(t, []byte("BAR"), v)
	})
}

// TestStoreDelete verifies Delete behaviour.
func TestStoreDelete(t *testing.T) {
	foo := encoding.EncodeInt64(nil, 1)
	bar := encoding.EncodeInt64(nil, 2)

	t.Run("Should delete the right object", func(t *testing.T) {
		st := kvBuilder(t)

		err := st.Put(foo, []byte("FOO"))
		require.NoError(t, err)
		err = st.Put(bar, []byte("BAR"))
		require.NoError(t, err)

		v := getValue(t, st, foo)
		require.Equal(t, []byte("FOO"), v)

		// delete the key
		err = st.Delete(bar)
		require.NoError(t, err)

		// try again, should fail
		ok, err := st.Exists(bar)
		require.NoError(t, err)
		require.False(t, ok)

		// make sure it didn't also delete the other one
		v = getValue(t, st, foo)
		require.Equal(t, []byte("FOO"), v)

		// the deleted key must not appear on iteration
		it, err := st.Iterator(nil)
		require.NoError(t, err)
		defer it.Close()
		for it.First(); it.Valid(); it.Next() {
			if bytes.Equal(it.Key(), bar) {
				t.Fatal("bar should not be present")
			}
		}
	})
}

// TestQueries test simple queries against the kv.
func TestQueries(t *testing.T) {
	t.Run("SELECT", func(t *testing.T) {
		dir := t.TempDir()

		db, err := sql.Open("chai", filepath.Join(dir, "pebble"))
		require.NoError(t, err)

		var count int
		err = db.QueryRow(`
			CREATE TABLE test(a INT);
			INSERT INTO test (a) VALUES (1), (2), (3), (4);
			SELECT COUNT(*) FROM test;
		`).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 4, count)

		t.Run("ORDER BY", func(t *testing.T) {
			rows, err := db.Query("SELECT * FROM test ORDER BY a DESC")
			require.NoError(t, err)
			defer rows.Close()

			var i int
			for rows.Next() {
				var a int
				err := rows.Scan(&a)
				require.NoError(t, err)
				require.Equal(t, 4-i, a)
				i++
			}
		})
	})

	t.Run("INSERT", func(t *testing.T) {
		dir := t.TempDir()

		db, err := sql.Open("chai", filepath.Join(dir, "pebble"))
		require.NoError(t, err)

		_, err = db.Exec(`
			CREATE TABLE test(a INT);
			INSERT INTO test (a) VALUES (1), (2), (3), (4);
		`)
		require.NoError(t, err)
	})

	t.Run("UPDATE", func(t *testing.T) {
		dir := t.TempDir()

		db, err := sql.Open("chai", filepath.Join(dir, "pebble"))
		require.NoError(t, err)

		rows, err := db.Query(`
			CREATE TABLE test(a INT);
			INSERT INTO test (a) VALUES (1), (2), (3), (4);
			UPDATE test SET a = 5;
			SELECT * FROM test;
		`)
		require.NoError(t, err)

		testutil.RequireJSONEq(t, rows, `{"a": 5}`, `{"a": 5}`, `{"a": 5}`, `{"a": 5}`)
	})

	t.Run("DELETE", func(t *testing.T) {
		dir := t.TempDir()

		db, err := sql.Open("chai", filepath.Join(dir, "pebble"))
		require.NoError(t, err)

		_, err = db.Exec("CREATE TABLE test(a INT)")
		require.NoError(t, err)

		tx, err := db.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		for i := 1; i < 200; i++ {
			_, err = tx.Exec("INSERT INTO test (a) VALUES ($1)", i)
			require.NoError(t, err)
		}

		err = tx.Commit()
		require.NoError(t, err)

		var count int
		err = db.QueryRow(`
			DELETE FROM test WHERE a > 2;
			SELECT COUNT(*) FROM test;
		`).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 2, count)
	})
}

// TestQueriesSameTransaction test simple queries in the same transaction.
func TestQueriesSameTransaction(t *testing.T) {
	t.Run("SELECT", func(t *testing.T) {
		dir := t.TempDir()

		db, err := sql.Open("chai", filepath.Join(dir, "pebble"))
		require.NoError(t, err)

		tx, err := db.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		var count int
		err = tx.QueryRow(`
				CREATE TABLE test(a INT);
				INSERT INTO test (a) VALUES (1), (2), (3), (4);
				SELECT COUNT(*) FROM test;
			`).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 4, count)

		err = tx.Commit()
		require.NoError(t, err)
	})

	t.Run("INSERT", func(t *testing.T) {
		dir := t.TempDir()

		db, err := sql.Open("chai", filepath.Join(dir, "pebble"))
		require.NoError(t, err)

		tx, err := db.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		_, err = tx.Exec(`
			CREATE TABLE test(a INT);
			INSERT INTO test (a) VALUES (1), (2), (3), (4);
		`)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)
	})

	t.Run("UPDATE", func(t *testing.T) {
		dir := t.TempDir()

		db, err := sql.Open("chai", filepath.Join(dir, "pebble"))
		require.NoError(t, err)

		tx, err := db.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		rows, err := tx.Query(`
				CREATE TABLE test(a INT);
				INSERT INTO test (a) VALUES (1), (2), (3), (4);
				UPDATE test SET a = 5;
				SELECT * FROM test;
			`)
		require.NoError(t, err)

		testutil.RequireJSONEq(t, rows, `{"a": 5}`, `{"a": 5}`, `{"a": 5}`, `{"a": 5}`)

		err = tx.Commit()
		require.NoError(t, err)
	})

	t.Run("DELETE", func(t *testing.T) {
		dir := t.TempDir()

		db, err := sql.Open("chai", filepath.Join(dir, "pebble"))
		require.NoError(t, err)

		tx, err := db.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		var count int
		err = tx.QueryRow(`
			CREATE TABLE test(a INT);
			INSERT INTO test (a) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
			DELETE FROM test WHERE a > 2;
			SELECT COUNT(*) FROM test;
		`).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 2, count)

		err = tx.Commit()
		require.NoError(t, err)
	})
}
