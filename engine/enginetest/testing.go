// Package enginetest defines a list of tests that can be used to test
// a complete or partial engine implementation.
package enginetest

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/asdine/genji"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/engine"
	"github.com/stretchr/testify/require"
)

// Builder is a function that can create an engine on demand and that provides
// a function to cleanup up and remove any created state.
// Tests will use the builder like this:
//     ng, cleanup := builder()
//     defer cleanup()
//     ...
type Builder func() (engine.Engine, func())

// TestSuite tests an entire engine, transaction and related types
// needed to implement a Genji engine.
func TestSuite(t *testing.T, builder Builder) {
	tests := []struct {
		name string
		test func(*testing.T, Builder)
	}{
		{"Engine", TestEngine},
		{"Transaction/Commit-Rollback", TestTransactionCommitRollback},
		{"Transaction/Store", TestTransactionStore},
		{"Transaction/CreateStore", TestTransactionCreateStore},
		{"Transaction/DropStore", TestTransactionDropStore},
		{"Transaction/ListStores", TestTransactionListStores},
		{"Store/Iterator", TestStoreIterator},
		{"Store/Put", TestStorePut},
		{"Store/Get", TestStoreGet},
		{"Store/Delete", TestStoreDelete},
		{"Store/Truncate", TestStoreTruncate},
		{"TestQueries", TestQueries},
		{"TestQueriesSameTransaction", TestQueriesSameTransaction},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.test(t, builder)
		})
	}
}

// TestEngine runs a list of tests against the provided engine.
func TestEngine(t *testing.T, builder Builder) {
	t.Run("Close", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		require.NoError(t, ng.Close())
	})
}

// TestTransactionCommitRollback runs a list of tests to verify Commit and Rollback
// behaviour of transactions created from the given engine.
func TestTransactionCommitRollback(t *testing.T, builder Builder) {
	ng, cleanup := builder()
	defer cleanup()

	t.Run("Commit on read-only transaction should fail", func(t *testing.T) {
		tx, err := ng.Begin(false)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.Commit()
		require.Error(t, err)
	})

	t.Run("Commit after rollback should fail", func(t *testing.T) {
		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.Rollback()
		require.NoError(t, err)

		err = tx.Commit()
		require.Error(t, err)
	})

	t.Run("Rollback after commit should not fail", func(t *testing.T) {
		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.Commit()
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)
	})

	t.Run("Commit after commit should fail", func(t *testing.T) {
		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.Commit()
		require.NoError(t, err)

		err = tx.Commit()
		require.Error(t, err)
	})

	t.Run("Rollback after rollback should not fail", func(t *testing.T) {
		tx, err := ng.Begin(false)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.Rollback()
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)
	})

	t.Run("Read-Only write attempts", func(t *testing.T) {
		tx, err := ng.Begin(true)
		require.NoError(t, err)

		// create store for testing store methods
		err = tx.CreateStore("store1")
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		// create a new read-only transaction
		tx, err = ng.Begin(false)
		defer tx.Rollback()

		// fetch the store and the index
		st, err := tx.GetStore("store1")
		require.NoError(t, err)

		tests := []struct {
			name string
			err  error
			fn   func(*error)
		}{
			{"CreateStore", engine.ErrTransactionReadOnly, func(err *error) { *err = tx.CreateStore("store") }},
			{"DropStore", engine.ErrTransactionReadOnly, func(err *error) { *err = tx.DropStore("store") }},
			{"StorePut", engine.ErrTransactionReadOnly, func(err *error) { *err = st.Put([]byte("id"), nil) }},
			{"StoreDelete", engine.ErrTransactionReadOnly, func(err *error) { *err = st.Delete([]byte("id")) }},
			{"StoreTruncate", engine.ErrTransactionReadOnly, func(err *error) { *err = st.Truncate() }},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				var err error
				test.fn(&err)

				require.Equal(t, test.err, err)
			})
		}
	})

	t.Run("Commit / Rollback data persistence", func(t *testing.T) {
		// this test checks if rollback undoes data changes correctly and if commit keeps data correctly
		tests := []struct {
			name    string
			initFn  func(engine.Transaction) error
			writeFn func(engine.Transaction, *error)
			readFn  func(engine.Transaction, *error)
		}{
			{
				"CreateStore",
				nil,
				func(tx engine.Transaction, err *error) { *err = tx.CreateStore("store") },
				func(tx engine.Transaction, err *error) { _, *err = tx.GetStore("store") },
			},
			{
				"DropStore",
				func(tx engine.Transaction) error { return tx.CreateStore("store") },
				func(tx engine.Transaction, err *error) { *err = tx.DropStore("store") },
				func(tx engine.Transaction, err *error) { *err = tx.CreateStore("store") },
			},
			{
				"StorePut",
				func(tx engine.Transaction) error { return tx.CreateStore("store") },
				func(tx engine.Transaction, err *error) {
					st, er := tx.GetStore("store")
					require.NoError(t, er)
					require.NoError(t, st.Put([]byte("foo"), []byte("FOO")))
				},
				func(tx engine.Transaction, err *error) {
					st, er := tx.GetStore("store")
					require.NoError(t, er)
					_, *err = st.Get([]byte("foo"))
				},
			},
		}

		for _, test := range tests {
			t.Run(test.name+"/rollback", func(t *testing.T) {
				ng, cleanup := builder()
				defer cleanup()

				if test.initFn != nil {
					func() {
						tx, err := ng.Begin(true)
						require.NoError(t, err)
						defer tx.Rollback()

						err = test.initFn(tx)
						require.NoError(t, err)
						err = tx.Commit()
						require.NoError(t, err)
					}()
				}

				tx, err := ng.Begin(true)
				require.NoError(t, err)
				defer tx.Rollback()

				test.writeFn(tx, &err)
				require.NoError(t, err)

				err = tx.Rollback()
				require.NoError(t, err)

				tx, err = ng.Begin(true)
				require.NoError(t, err)
				defer tx.Rollback()

				test.readFn(tx, &err)
				require.Error(t, err)
			})
		}

		for _, test := range tests {
			ng, cleanup := builder()
			defer cleanup()

			t.Run(test.name+"/commit", func(t *testing.T) {
				if test.initFn != nil {
					func() {
						tx, err := ng.Begin(true)
						require.NoError(t, err)
						defer tx.Rollback()

						err = test.initFn(tx)
						require.NoError(t, err)
						err = tx.Commit()
						require.NoError(t, err)
					}()
				}

				tx, err := ng.Begin(true)
				require.NoError(t, err)
				defer tx.Rollback()

				test.writeFn(tx, &err)
				require.NoError(t, err)

				err = tx.Commit()
				require.NoError(t, err)

				tx, err = ng.Begin(true)
				require.NoError(t, err)
				defer tx.Rollback()

				test.readFn(tx, &err)
				require.NoError(t, err)
			})
		}
	})

	t.Run("Data should be visible within the same transaction", func(t *testing.T) {
		tests := []struct {
			name    string
			writeFn func(engine.Transaction, *error)
			readFn  func(engine.Transaction, *error)
		}{
			{
				"CreateStore",
				func(tx engine.Transaction, err *error) { *err = tx.CreateStore("store") },
				func(tx engine.Transaction, err *error) { _, *err = tx.GetStore("store") },
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				ng, cleanup := builder()
				defer cleanup()

				tx, err := ng.Begin(true)
				require.NoError(t, err)
				defer tx.Rollback()

				test.writeFn(tx, &err)
				require.NoError(t, err)

				test.readFn(tx, &err)
				require.NoError(t, err)
			})
		}
	})
}

// TestTransactionCreateStore verifies CreateStore behaviour.
func TestTransactionCreateStore(t *testing.T, builder Builder) {
	t.Run("Should create a store", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.CreateStore("store")
		require.NoError(t, err)

		st, err := tx.GetStore("store")
		require.NoError(t, err)
		require.NotNil(t, st)
	})

	t.Run("Should fail if store already exists", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.CreateStore("store")
		require.NoError(t, err)
		err = tx.CreateStore("store")
		require.Equal(t, engine.ErrStoreAlreadyExists, err)
	})
}

// TestTransactionStore verifies Store behaviour.
func TestTransactionStore(t *testing.T, builder Builder) {
	t.Run("Should fail if store not found", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(false)
		require.NoError(t, err)
		defer tx.Rollback()

		_, err = tx.GetStore("store")
		require.Equal(t, engine.ErrStoreNotFound, err)
	})

	t.Run("Should return the right store", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		// create two stores
		err = tx.CreateStore("storea")
		require.NoError(t, err)

		err = tx.CreateStore("storeb")
		require.NoError(t, err)

		// fetch first store
		sta, err := tx.GetStore("storea")
		require.NoError(t, err)

		// fetch second store
		stb, err := tx.GetStore("storeb")
		require.NoError(t, err)

		// insert data in first store
		err = sta.Put([]byte("foo"), []byte("FOO"))
		require.NoError(t, err)

		// use sta to fetch data and verify if it's present
		v, err := sta.Get([]byte("foo"))
		require.NoError(t, err)
		require.Equal(t, v, []byte("FOO"))

		// use stb to fetch data and verify it's not present
		_, err = stb.Get([]byte("foo"))
		require.Equal(t, engine.ErrKeyNotFound, err)
	})
}

// TestTransactionDropStore verifies DropStore behaviour.
func TestTransactionDropStore(t *testing.T, builder Builder) {
	t.Run("Should drop a store", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.CreateStore("store")
		require.NoError(t, err)

		err = tx.DropStore("store")
		require.NoError(t, err)

		_, err = tx.GetStore("store")
		require.Equal(t, engine.ErrStoreNotFound, err)
	})

	t.Run("Should fail if store not found", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.DropStore("store")
		require.Equal(t, engine.ErrStoreNotFound, err)
	})
}

// TestTransactionListStores verifies ListStores behaviour.
func TestTransactionListStores(t *testing.T, builder Builder) {
	t.Run("With no prefix, should list all stores", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		for i := 0; i < 10; i++ {
			err = tx.CreateStore(fmt.Sprintf("store%d", i))
			require.NoError(t, err)
		}

		list, err := tx.ListStores("")
		require.NoError(t, err)
		require.Len(t, list, 10)
		for i, name := range list {
			require.Equal(t, fmt.Sprintf("store%d", i), name)
		}
	})

	t.Run("With a prefix, should list some stores", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		for i := 0; i < 10; i++ {
			if i%2 == 0 {
				err = tx.CreateStore(fmt.Sprintf("foo%d", i))
			} else {
				err = tx.CreateStore(fmt.Sprintf("bar%d", i))
			}
			require.NoError(t, err)
		}

		list, err := tx.ListStores("f")
		require.NoError(t, err)
		require.Len(t, list, 5)
		for i, name := range list {
			require.Equal(t, fmt.Sprintf("foo%d", i*2), name)
		}
	})
}

func storeBuilder(t testing.TB, builder Builder) (engine.Store, func()) {
	ng, cleanup := builder()
	tx, err := ng.Begin(true)
	require.NoError(t, err)
	err = tx.CreateStore("test")
	require.NoError(t, err)
	st, err := tx.GetStore("test")
	require.NoError(t, err)
	return st, func() {
		tx.Rollback()
		cleanup()
	}
}

// TestStoreIterator verifies Iterator behaviour.
func TestStoreIterator(t *testing.T, builder Builder) {
	t.Run("Should not fail with no documents", func(t *testing.T) {
		fn := func(t *testing.T, reverse bool) {
			st, cleanup := storeBuilder(t, builder)
			defer cleanup()

			it := st.NewIterator(engine.IteratorConfig{Reverse: reverse})
			defer it.Close()
			i := 0

			for it.Seek(nil); it.Valid(); it.Next() {
				i++
			}
			require.Zero(t, i)
		}
		t.Run("Reverse: false", func(t *testing.T) {
			fn(t, false)
		})
		t.Run("Reverse: true", func(t *testing.T) {
			fn(t, true)
		})
	})

	t.Run("With no pivot, should iterate over all documents in order", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		for i := 1; i <= 10; i++ {
			err := st.Put([]byte{uint8(i)}, []byte{uint8(i + 20)})
			require.NoError(t, err)
		}

		var i uint8 = 1
		var count int
		it := st.NewIterator(engine.IteratorConfig{})
		defer it.Close()

		for it.Seek(nil); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			v, _ := item.ValueCopy(nil)
			require.Equal(t, []byte{i}, k)
			require.Equal(t, []byte{i + 20}, v)
			i++
			count++
		}

		require.Equal(t, count, 10)
	})

	t.Run("With no pivot, should iterate over all documents in reverse order", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		for i := 1; i <= 10; i++ {
			err := st.Put([]byte{uint8(i)}, []byte{uint8(i + 20)})
			require.NoError(t, err)
		}

		var i uint8 = 10
		var count int
		it := st.NewIterator(engine.IteratorConfig{Reverse: true})
		defer it.Close()

		for it.Seek(nil); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			v, _ := item.ValueCopy(nil)
			require.Equal(t, []byte{i}, k)
			require.Equal(t, []byte{i + 20}, v)
			i--
			count++
		}
		require.Equal(t, 10, count)
	})

	t.Run("With pivot, should iterate over some documents in order", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		for i := 1; i <= 10; i++ {
			err := st.Put([]byte{uint8(i)}, []byte{uint8(i + 20)})
			require.NoError(t, err)
		}

		var i uint8 = 4
		var count int
		it := st.NewIterator(engine.IteratorConfig{})
		defer it.Close()

		for it.Seek([]byte{i}); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			v, _ := item.ValueCopy(nil)
			require.Equal(t, []byte{i}, k)
			require.Equal(t, []byte{i + 20}, v)
			i++
			count++
		}
		require.Equal(t, 7, count)
	})

	t.Run("With pivot, should iterate over some documents in reverse order", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		for i := 1; i <= 10; i++ {
			err := st.Put([]byte{uint8(i)}, []byte{uint8(i + 20)})
			require.NoError(t, err)
		}

		var i uint8 = 4
		var count int
		it := st.NewIterator(engine.IteratorConfig{Reverse: true})
		defer it.Close()

		for it.Seek([]byte{i}); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			v, _ := item.ValueCopy(nil)
			require.Equal(t, []byte{i}, k)
			require.Equal(t, []byte{i + 20}, v)
			i--
			count++
		}
		require.Equal(t, 4, count)
	})

	t.Run("If pivot not found, should start from the next item", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		err := st.Put([]byte{1}, []byte{1})
		require.NoError(t, err)

		err = st.Put([]byte{3}, []byte{3})
		require.NoError(t, err)

		called := false
		it := st.NewIterator(engine.IteratorConfig{})
		defer it.Close()

		for it.Seek([]byte{2}); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			v, _ := item.ValueCopy(nil)
			require.Equal(t, []byte{3}, k)
			require.Equal(t, []byte{3}, v)
			called = true
		}

		require.True(t, called)
	})

	t.Run("With reverse true, if pivot not found, should start from the previous item", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		err := st.Put([]byte{1}, []byte{1})
		require.NoError(t, err)

		err = st.Put([]byte{3}, []byte{3})
		require.NoError(t, err)

		called := false
		it := st.NewIterator(engine.IteratorConfig{Reverse: true})
		defer it.Close()

		for it.Seek([]byte{2}); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			v, _ := item.ValueCopy(nil)
			require.Equal(t, []byte{1}, k)
			require.Equal(t, []byte{1}, v)
			called = true
		}
		require.True(t, called)
	})
}

// TestStorePut verifies Put behaviour.
func TestStorePut(t *testing.T, builder Builder) {
	t.Run("Should insert data", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		err := st.Put([]byte("foo"), []byte("FOO"))
		require.NoError(t, err)

		v, err := st.Get([]byte("foo"))
		require.NoError(t, err)
		require.Equal(t, []byte("FOO"), v)
	})

	t.Run("Should replace existing key", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		err := st.Put([]byte("foo"), []byte("FOO"))
		require.NoError(t, err)

		err = st.Put([]byte("foo"), []byte("BAR"))
		require.NoError(t, err)

		v, err := st.Get([]byte("foo"))
		require.NoError(t, err)
		require.Equal(t, []byte("BAR"), v)
	})

	t.Run("Should fail when key is nil or empty", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		err := st.Put(nil, []byte("FOO"))
		require.Error(t, err)

		err = st.Put([]byte(""), []byte("BAR"))
		require.Error(t, err)
	})

	t.Run("Should succeed when value is nil or empty", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		err := st.Put([]byte("foo"), nil)
		require.NoError(t, err)

		err = st.Put([]byte("foo"), []byte(""))
		require.NoError(t, err)
	})
}

// TestStoreGet verifies Get behaviour.
func TestStoreGet(t *testing.T, builder Builder) {
	t.Run("Should fail if not found", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		r, err := st.Get([]byte("id"))
		require.Equal(t, engine.ErrKeyNotFound, err)
		require.Nil(t, r)
	})

	t.Run("Should return the right key", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		err := st.Put([]byte("foo"), []byte("FOO"))
		require.NoError(t, err)
		err = st.Put([]byte("bar"), []byte("BAR"))
		require.NoError(t, err)

		v, err := st.Get([]byte("foo"))
		require.NoError(t, err)
		require.Equal(t, []byte("FOO"), v)

		v, err = st.Get([]byte("bar"))
		require.NoError(t, err)
		require.Equal(t, []byte("BAR"), v)
	})
}

// TestStoreDelete verifies Delete behaviour.
func TestStoreDelete(t *testing.T, builder Builder) {
	t.Run("Should fail if not found", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		err := st.Delete([]byte("id"))
		require.Equal(t, engine.ErrKeyNotFound, err)
	})

	t.Run("Should delete the right document", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		err := st.Put([]byte("foo"), []byte("FOO"))
		require.NoError(t, err)
		err = st.Put([]byte("bar"), []byte("BAR"))
		require.NoError(t, err)

		v, err := st.Get([]byte("foo"))
		require.NoError(t, err)
		require.Equal(t, []byte("FOO"), v)

		// delete the key
		err = st.Delete([]byte("foo"))
		require.NoError(t, err)

		// try again, should fail
		err = st.Delete([]byte("foo"))
		require.Equal(t, engine.ErrKeyNotFound, err)

		// make sure it didn't also delete the other one
		v, err = st.Get([]byte("bar"))
		require.NoError(t, err)
		require.Equal(t, []byte("BAR"), v)
	})
}

// TestStoreTruncate verifies Truncate behaviour.
func TestStoreTruncate(t *testing.T, builder Builder) {
	t.Run("Should succeed if store is empty", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		err := st.Truncate()
		require.NoError(t, err)
	})

	t.Run("Should truncate the store", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		err := st.Put([]byte("foo"), []byte("FOO"))
		require.NoError(t, err)
		err = st.Put([]byte("bar"), []byte("BAR"))
		require.NoError(t, err)

		err = st.Truncate()
		require.NoError(t, err)

		it := st.NewIterator(engine.IteratorConfig{})
		defer it.Close()
		it.Seek(nil)
		require.False(t, it.Valid())
	})
}

// TestQueries test simple queries against the engine.
func TestQueries(t *testing.T, builder Builder) {
	t.Run("SELECT", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		db, err := genji.New(ng)
		require.NoError(t, err)
		defer db.Close()

		st, err := db.Query(`
			CREATE TABLE test;
			INSERT INTO test (a) VALUES (1), (2), (3), (4);
			SELECT * FROM test;
		`)
		require.NoError(t, err)
		n, err := st.Count()
		require.NoError(t, err)
		require.Equal(t, 4, n)
		err = st.Close()
		require.NoError(t, err)

		t.Run("ORDER BY", func(t *testing.T) {
			st, err := db.Query("SELECT * FROM test ORDER BY a DESC")
			require.NoError(t, err)
			defer st.Close()

			var i int
			err = st.Iterate(func(d document.Document) error {
				var a int
				err := document.Scan(d, &a)
				require.NoError(t, err)
				require.Equal(t, 4-i, a)
				i++
				return nil
			})
			require.NoError(t, err)
		})
	})

	t.Run("INSERT", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		db, err := genji.New(ng)
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec(`
			CREATE TABLE test;
			INSERT INTO test (a) VALUES (1), (2), (3), (4);
		`)
		require.NoError(t, err)
	})

	t.Run("UPDATE", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		db, err := genji.New(ng)
		require.NoError(t, err)
		defer db.Close()

		st, err := db.Query(`
				CREATE TABLE test;
				INSERT INTO test (a) VALUES (1), (2), (3), (4);
				UPDATE test SET a = 5;
				SELECT * FROM test;
			`)
		require.NoError(t, err)
		defer st.Close()
		var buf bytes.Buffer
		err = document.IteratorToJSONArray(&buf, st)
		require.NoError(t, err)
		require.JSONEq(t, `[{"a": 5},{"a": 5},{"a": 5},{"a": 5}]`, buf.String())
	})

	t.Run("DELETE", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		db, err := genji.New(ng)
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec("CREATE TABLE test")
		require.NoError(t, err)

		err = db.Update(func(tx *genji.Tx) error {
			for i := 1; i < 200; i++ {
				err = tx.Exec("INSERT INTO test (a) VALUES (?)", i)
				require.NoError(t, err)
			}
			return nil
		})
		require.NoError(t, err)

		st, err := db.Query(`
			DELETE FROM test WHERE a > 2;
			SELECT * FROM test;
		`)
		require.NoError(t, err)
		defer st.Close()
		n, err := st.Count()
		require.NoError(t, err)
		require.Equal(t, 2, n)
	})
}

// TestQueriesSameTransaction test simple queries in the same transaction.
func TestQueriesSameTransaction(t *testing.T, builder Builder) {
	t.Run("SELECT", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		db, err := genji.New(ng)
		require.NoError(t, err)
		defer db.Close()

		err = db.Update(func(tx *genji.Tx) error {
			st, err := tx.Query(`
				CREATE TABLE test;
				INSERT INTO test (a) VALUES (1), (2), (3), (4);
				SELECT * FROM test;
			`)
			require.NoError(t, err)
			defer st.Close()
			n, err := st.Count()
			require.NoError(t, err)
			require.Equal(t, 4, n)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("INSERT", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		db, err := genji.New(ng)
		require.NoError(t, err)
		defer db.Close()

		err = db.Update(func(tx *genji.Tx) error {
			err = tx.Exec(`
			CREATE TABLE test;
			INSERT INTO test (a) VALUES (1), (2), (3), (4);
		`)
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("UPDATE", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		db, err := genji.New(ng)
		require.NoError(t, err)
		defer db.Close()

		err = db.Update(func(tx *genji.Tx) error {
			st, err := tx.Query(`
				CREATE TABLE test;
				INSERT INTO test (a) VALUES (1), (2), (3), (4);
				UPDATE test SET a = 5;
				SELECT * FROM test;
			`)
			require.NoError(t, err)
			defer st.Close()
			var buf bytes.Buffer
			err = document.IteratorToJSONArray(&buf, st)
			require.NoError(t, err)
			require.JSONEq(t, `[{"a": 5},{"a": 5},{"a": 5},{"a": 5}]`, buf.String())
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("DELETE", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		db, err := genji.New(ng)
		require.NoError(t, err)
		defer db.Close()

		err = db.Update(func(tx *genji.Tx) error {
			st, err := tx.Query(`
			CREATE TABLE test;
			INSERT INTO test (a) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
			DELETE FROM test WHERE a > 2;
			SELECT * FROM test;
		`)
			require.NoError(t, err)
			defer st.Close()
			n, err := st.Count()
			require.NoError(t, err)
			require.Equal(t, 2, n)
			return nil
		})
		require.NoError(t, err)
	})
}
