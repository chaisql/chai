package kv_test

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/chaisql/chai"
	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/encoding"
	"github.com/chaisql/chai/internal/kv"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func getValue(t *testing.T, st kv.Session, key []byte) []byte {
	v, err := st.Get([]byte(key))
	assert.NoError(t, err)
	return v
}

func TestReadOnly(t *testing.T) {
	pdb := testutil.NewPebble(t)

	t.Run("Read-Only write attempts", func(t *testing.T) {
		sro := kv.NewStore(pdb, kv.Options{}).NewSnapshotSession()
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

				assert.Error(t, err)
			})
		}
	})
}

func kvBuilder(t testing.TB) kv.Session {
	pdb := testutil.NewPebble(t)

	s := kv.NewStore(pdb, kv.Options{
		RollbackSegmentNamespace: int64(database.RollbackSegmentNamespace),
		MaxBatchSize:             1 << 7,
	}).NewBatchSession()
	t.Cleanup(func() {
		s.Close()
	})

	return s
}

func TestBatchCommit(t *testing.T) {
	pdb := testutil.NewPebble(t)

	store := kv.NewStore(pdb, kv.Options{
		RollbackSegmentNamespace: int64(database.RollbackSegmentNamespace),
		MaxBatchSize:             1 << 7,
	})
	batch := store.NewBatchSession()
	defer batch.Close()

	var k int64
	for i := int64(0); i < 10; i++ {
		for j := int64(0); j < 10; j++ {
			k++
			key := encoding.EncodeInt(encoding.EncodeInt(nil, 10), j)
			err := batch.Put(key, encoding.EncodeInt(nil, k))
			assert.NoError(t, err)
		}
	}

	// snapshots created during the write transaction should not see the changes
	ss := store.NewSnapshotSession()
	_, err := ss.Get(encoding.EncodeInt(encoding.EncodeInt(nil, 10), 9))
	require.Error(t, err)
	err = ss.Close()
	require.NoError(t, err)

	// commit the write transaction
	err = batch.Commit()
	require.NoError(t, err)

	// try to read again, should see the changes
	ss = store.NewSnapshotSession()
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
	pdb := testutil.NewPebble(t)

	store := kv.NewStore(pdb, kv.Options{
		RollbackSegmentNamespace: int64(database.RollbackSegmentNamespace),
		MaxBatchSize:             1 << 7,
	})
	s := store.NewBatchSession()
	defer s.Close()

	var k int64
	for i := int64(0); i < 10; i++ {
		for j := int64(0); j < 10; j++ {
			k++
			key := encoding.EncodeInt(encoding.EncodeInt(nil, 10), j)
			err := s.Put(key, encoding.EncodeInt(nil, k))
			assert.NoError(t, err)
		}
	}

	err := s.Close()
	require.NoError(t, err)

	err = store.Rollback()
	require.NoError(t, err)

	for i := int64(9); i >= 0; i-- {
		key := encoding.EncodeInt(encoding.EncodeInt(nil, 10), i)
		_, _, err = pdb.Get(key)
		require.Equal(t, pebble.ErrNotFound, err)
	}
}

func TestStorePut(t *testing.T) {
	t.Run("Should insert data", func(t *testing.T) {
		st := kvBuilder(t)

		err := st.Put([]byte("foo"), []byte("FOO"))
		assert.NoError(t, err)

		v := getValue(t, st, []byte("foo"))
		require.Equal(t, []byte("FOO"), v)
	})

	t.Run("Should replace existing key", func(t *testing.T) {
		st := kvBuilder(t)

		err := st.Put([]byte("foo"), []byte("FOO"))
		assert.NoError(t, err)

		err = st.Put([]byte("foo"), []byte("BAR"))
		assert.NoError(t, err)

		v := getValue(t, st, []byte("foo"))
		require.Equal(t, []byte("BAR"), v)
	})

	t.Run("Should fail when key is nil or empty", func(t *testing.T) {
		st := kvBuilder(t)

		err := st.Put(nil, []byte("FOO"))
		assert.Error(t, err)

		err = st.Put([]byte(""), []byte("BAR"))
		assert.Error(t, err)
	})

	t.Run("Should fail when value is nil or empty", func(t *testing.T) {
		st := kvBuilder(t)

		err := st.Put([]byte("foo"), nil)
		assert.Error(t, err)

		err = st.Put([]byte("foo"), []byte(""))
		assert.Error(t, err)
	})
}

// TestStoreGet verifies Get behaviour.
func TestStoreGet(t *testing.T) {
	t.Run("Should fail if not found", func(t *testing.T) {
		st := kvBuilder(t)

		r, err := st.Get([]byte("id"))
		assert.ErrorIs(t, err, kv.ErrKeyNotFound)
		require.Nil(t, r)
	})

	t.Run("Should return the right key", func(t *testing.T) {
		st := kvBuilder(t)

		err := st.Put([]byte("foo"), []byte("FOO"))
		assert.NoError(t, err)
		err = st.Put([]byte("bar"), []byte("BAR"))
		assert.NoError(t, err)

		v := getValue(t, st, []byte("foo"))
		require.Equal(t, []byte("FOO"), v)

		v = getValue(t, st, []byte("bar"))
		require.Equal(t, []byte("BAR"), v)
	})
}

// TestStoreDelete verifies Delete behaviour.
func TestStoreDelete(t *testing.T) {
	t.Run("Should delete the right object", func(t *testing.T) {
		st := kvBuilder(t)

		err := st.Put([]byte("foo"), []byte("FOO"))
		assert.NoError(t, err)
		err = st.Put([]byte("bar"), []byte("BAR"))
		assert.NoError(t, err)

		v := getValue(t, st, []byte("foo"))
		require.Equal(t, []byte("FOO"), v)

		// delete the key
		err = st.Delete([]byte("bar"))
		assert.NoError(t, err)

		// try again, should fail
		ok, err := st.Exists([]byte("bar"))
		assert.NoError(t, err)
		require.False(t, ok)

		// make sure it didn't also delete the other one
		v = getValue(t, st, []byte("foo"))
		require.Equal(t, []byte("FOO"), v)

		// the deleted key must not appear on iteration
		it, err := st.Iterator(nil)
		assert.NoError(t, err)
		defer it.Close()
		for it.First(); it.Valid(); it.Next() {
			if bytes.Equal(it.Key(), []byte("bar")) {
				t.Fatal("bar should not be present")
			}
		}
	})
}

// TestQueries test simple queries against the kv.
func TestQueries(t *testing.T) {
	t.Run("SELECT", func(t *testing.T) {
		dir := t.TempDir()

		db, err := chai.Open(filepath.Join(dir, "pebble"))
		assert.NoError(t, err)

		r, err := db.QueryRow(`
			CREATE TABLE test;
			INSERT INTO test (a) VALUES (1), (2), (3), (4);
			SELECT COUNT(*) FROM test;
		`)
		assert.NoError(t, err)
		var count int
		err = r.Scan(&count)
		assert.NoError(t, err)
		require.Equal(t, 4, count)

		t.Run("ORDER BY", func(t *testing.T) {
			st, err := db.Query("SELECT * FROM test ORDER BY a DESC")
			assert.NoError(t, err)
			defer st.Close()

			var i int
			err = st.Iterate(func(r *chai.Row) error {
				var a int
				err := r.Scan(&a)
				assert.NoError(t, err)
				require.Equal(t, 4-i, a)
				i++
				return nil
			})
			assert.NoError(t, err)
		})
	})

	t.Run("INSERT", func(t *testing.T) {
		dir := t.TempDir()

		db, err := chai.Open(filepath.Join(dir, "pebble"))
		assert.NoError(t, err)

		err = db.Exec(`
			CREATE TABLE test;
			INSERT INTO test (a) VALUES (1), (2), (3), (4);
		`)
		assert.NoError(t, err)
	})

	t.Run("UPDATE", func(t *testing.T) {
		dir := t.TempDir()

		db, err := chai.Open(filepath.Join(dir, "pebble"))
		assert.NoError(t, err)

		st, err := db.Query(`
				CREATE TABLE test;
				INSERT INTO test (a) VALUES (1), (2), (3), (4);
				UPDATE test SET a = 5;
				SELECT * FROM test;
			`)
		assert.NoError(t, err)
		defer st.Close()
		var buf bytes.Buffer
		err = st.MarshalJSONTo(&buf)
		assert.NoError(t, err)
		require.JSONEq(t, `[{"a": 5},{"a": 5},{"a": 5},{"a": 5}]`, buf.String())
	})

	t.Run("DELETE", func(t *testing.T) {
		dir := t.TempDir()

		db, err := chai.Open(filepath.Join(dir, "pebble"))
		assert.NoError(t, err)

		err = db.Exec("CREATE TABLE test")
		assert.NoError(t, err)

		err = db.Update(func(tx *chai.Tx) error {
			for i := 1; i < 200; i++ {
				err = tx.Exec("INSERT INTO test (a) VALUES (?)", i)
				assert.NoError(t, err)
			}
			return nil
		})
		assert.NoError(t, err)

		r, err := db.QueryRow(`
			DELETE FROM test WHERE a > 2;
			SELECT COUNT(*) FROM test;
		`)
		assert.NoError(t, err)
		var count int
		err = r.Scan(&count)
		assert.NoError(t, err)
		require.Equal(t, 2, count)
	})
}

// TestQueriesSameTransaction test simple queries in the same transaction.
func TestQueriesSameTransaction(t *testing.T) {
	t.Run("SELECT", func(t *testing.T) {
		dir := t.TempDir()

		db, err := chai.Open(filepath.Join(dir, "pebble"))
		assert.NoError(t, err)

		err = db.Update(func(tx *chai.Tx) error {
			r, err := tx.QueryRow(`
				CREATE TABLE test;
				INSERT INTO test (a) VALUES (1), (2), (3), (4);
				SELECT COUNT(*) FROM test;
			`)
			assert.NoError(t, err)
			var count int
			err = r.Scan(&count)
			assert.NoError(t, err)
			require.Equal(t, 4, count)
			return nil
		})
		assert.NoError(t, err)
	})

	t.Run("INSERT", func(t *testing.T) {
		dir := t.TempDir()

		db, err := chai.Open(filepath.Join(dir, "pebble"))
		assert.NoError(t, err)

		err = db.Update(func(tx *chai.Tx) error {
			err = tx.Exec(`
			CREATE TABLE test;
			INSERT INTO test (a) VALUES (1), (2), (3), (4);
		`)
			assert.NoError(t, err)
			return nil
		})
		assert.NoError(t, err)
	})

	t.Run("UPDATE", func(t *testing.T) {
		dir := t.TempDir()

		db, err := chai.Open(filepath.Join(dir, "pebble"))
		assert.NoError(t, err)

		err = db.Update(func(tx *chai.Tx) error {
			st, err := tx.Query(`
				CREATE TABLE test;
				INSERT INTO test (a) VALUES (1), (2), (3), (4);
				UPDATE test SET a = 5;
				SELECT * FROM test;
			`)
			assert.NoError(t, err)
			defer st.Close()
			var buf bytes.Buffer
			err = st.MarshalJSONTo(&buf)
			assert.NoError(t, err)
			require.JSONEq(t, `[{"a": 5},{"a": 5},{"a": 5},{"a": 5}]`, buf.String())
			return nil
		})
		assert.NoError(t, err)
	})

	t.Run("DELETE", func(t *testing.T) {
		dir := t.TempDir()

		db, err := chai.Open(filepath.Join(dir, "pebble"))
		assert.NoError(t, err)

		err = db.Update(func(tx *chai.Tx) error {
			r, err := tx.QueryRow(`
			CREATE TABLE test;
			INSERT INTO test (a) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
			DELETE FROM test WHERE a > 2;
			SELECT COUNT(*) FROM test;
		`)
			assert.NoError(t, err)
			var count int
			err = r.Scan(&count)
			assert.NoError(t, err)
			require.Equal(t, 2, count)
			return nil
		})
		assert.NoError(t, err)
	})
}
