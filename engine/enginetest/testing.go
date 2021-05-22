// Package enginetest defines a list of tests that can be used to test
// a complete or partial engine implementation.
package enginetest

import (
	"bytes"
	"context"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/stretchr/testify/require"
)

// Builder is a function that can create an engine on demand and that provides
// a function to cleanup up and remove any created state. Note that the engine
// is not closed on cleanup.
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
		{"Transaction/GetStore", TestTransactionGetStore},
		{"Transaction/CreateStore", TestTransactionCreateStore},
		{"Transaction/DropStore", TestTransactionDropStore},
		{"Store/Iterator", TestStoreIterator},
		{"Store/Put", TestStorePut},
		{"Store/Get", TestStoreGet},
		{"Store/Delete", TestStoreDelete},
		{"Store/Truncate", TestStoreTruncate},
		{"Store/NextSequence", TestStoreNextSequence},
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
	defer func() {
		require.NoError(t, ng.Close())
	}()

	t.Run("Commit on read-only transaction should fail", func(t *testing.T) {
		tx, err := ng.Begin(context.Background(), engine.TxOptions{
			Writable: false,
		})
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.Commit()
		require.Error(t, err)
	})

	t.Run("Commit after rollback should fail", func(t *testing.T) {
		tx, err := ng.Begin(context.Background(), engine.TxOptions{
			Writable: true,
		})
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.Rollback()
		require.NoError(t, err)

		err = tx.Commit()
		require.Error(t, err)
	})

	t.Run("Commit after context canceled should fail", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		tx, err := ng.Begin(ctx, engine.TxOptions{
			Writable: true,
		})
		require.NoError(t, err)

		require.NoError(t, tx.CreateStore([]byte("test")))
		st, err := tx.GetStore([]byte("test"))
		require.NoError(t, err)
		err = st.Put([]byte("a"), []byte("b"))
		require.NoError(t, err)

		cancel()

		err = tx.Commit()
		require.Error(t, err)

		// ensure data has not been persisted
		tx, err = ng.Begin(context.Background(), engine.TxOptions{
			Writable: false,
		})
		require.NoError(t, err)
		defer tx.Rollback()

		st, err = tx.GetStore([]byte("test"))
		require.Error(t, err)
	})

	t.Run("Rollback after commit should return ErrTransactionDiscarded", func(t *testing.T) {
		tx, err := ng.Begin(context.Background(), engine.TxOptions{
			Writable: true,
		})
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.Commit()
		require.NoError(t, err)

		err = tx.Rollback()
		require.Equal(t, engine.ErrTransactionDiscarded, err)
	})

	t.Run("Commit after commit should return ErrTransactionDiscarded", func(t *testing.T) {
		tx, err := ng.Begin(context.Background(), engine.TxOptions{
			Writable: true,
		})
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.Commit()
		require.NoError(t, err)

		err = tx.Commit()
		require.Equal(t, engine.ErrTransactionDiscarded, err)
	})

	t.Run("Rollback after rollback should should return ErrTransactionDiscarded", func(t *testing.T) {
		tx, err := ng.Begin(context.Background(), engine.TxOptions{
			Writable: false,
		})
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.Rollback()
		require.NoError(t, err)

		err = tx.Rollback()
		require.Equal(t, engine.ErrTransactionDiscarded, err)
	})

	t.Run("Rollback after context canceled should return context.Canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		tx, err := ng.Begin(ctx, engine.TxOptions{
			Writable: true,
		})
		require.NoError(t, err)

		cancel()

		err = tx.Rollback()
		require.Equal(t, context.Canceled, err)
	})

	t.Run("Read-Only write attempts", func(t *testing.T) {
		tx, err := ng.Begin(context.Background(), engine.TxOptions{
			Writable: true,
		})
		require.NoError(t, err)

		// create store for testing store methods
		err = tx.CreateStore([]byte("store1"))
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		// create a new read-only transaction
		tx, err = ng.Begin(context.Background(), engine.TxOptions{
			Writable: false,
		})
		defer tx.Rollback()

		// fetch the store and the index
		st, err := tx.GetStore([]byte("store1"))
		require.NoError(t, err)

		tests := []struct {
			name string
			err  error
			fn   func(*error)
		}{
			{"CreateStore", engine.ErrTransactionReadOnly, func(err *error) { *err = tx.CreateStore([]byte("store")) }},
			{"DropStore", engine.ErrTransactionReadOnly, func(err *error) { *err = tx.DropStore([]byte("store")) }},
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
				func(tx engine.Transaction, err *error) { *err = tx.CreateStore([]byte("store")) },
				func(tx engine.Transaction, err *error) { _, *err = tx.GetStore([]byte("store")) },
			},
			{
				"DropStore",
				func(tx engine.Transaction) error { return tx.CreateStore([]byte("store")) },
				func(tx engine.Transaction, err *error) { *err = tx.DropStore([]byte("store")) },
				func(tx engine.Transaction, err *error) { *err = tx.CreateStore([]byte("store")) },
			},
			{
				"StorePut",
				func(tx engine.Transaction) error { return tx.CreateStore([]byte("store")) },
				func(tx engine.Transaction, err *error) {
					st, er := tx.GetStore([]byte("store"))
					require.NoError(t, er)
					require.NoError(t, st.Put([]byte("foo"), []byte("FOO")))
				},
				func(tx engine.Transaction, err *error) {
					st, er := tx.GetStore([]byte("store"))
					require.NoError(t, er)
					_, *err = st.Get([]byte("foo"))
				},
			},
		}

		for _, test := range tests {
			t.Run(test.name+"/rollback", func(t *testing.T) {
				ng, cleanup := builder()
				defer cleanup()
				defer func() {
					require.NoError(t, ng.Close())
				}()

				if test.initFn != nil {
					func() {
						tx, err := ng.Begin(context.Background(), engine.TxOptions{
							Writable: true,
						})
						require.NoError(t, err)
						defer tx.Rollback()

						err = test.initFn(tx)
						require.NoError(t, err)
						err = tx.Commit()
						require.NoError(t, err)
					}()
				}

				tx, err := ng.Begin(context.Background(), engine.TxOptions{
					Writable: true,
				})
				require.NoError(t, err)
				defer tx.Rollback()

				test.writeFn(tx, &err)
				require.NoError(t, err)

				err = tx.Rollback()
				require.NoError(t, err)

				tx, err = ng.Begin(context.Background(), engine.TxOptions{
					Writable: true,
				})
				require.NoError(t, err)
				defer tx.Rollback()

				test.readFn(tx, &err)
				require.Error(t, err)
			})
		}

		for _, test := range tests {
			ng, cleanup := builder()
			defer cleanup()
			defer func() {
				require.NoError(t, ng.Close())
			}()

			t.Run(test.name+"/commit", func(t *testing.T) {
				if test.initFn != nil {
					func() {
						tx, err := ng.Begin(context.Background(), engine.TxOptions{
							Writable: true,
						})
						require.NoError(t, err)
						defer tx.Rollback()

						err = test.initFn(tx)
						require.NoError(t, err)
						err = tx.Commit()
						require.NoError(t, err)
					}()
				}

				tx, err := ng.Begin(context.Background(), engine.TxOptions{
					Writable: true,
				})
				require.NoError(t, err)
				defer tx.Rollback()

				test.writeFn(tx, &err)
				require.NoError(t, err)

				err = tx.Commit()
				require.NoError(t, err)

				tx, err = ng.Begin(context.Background(), engine.TxOptions{
					Writable: true,
				})
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
				func(tx engine.Transaction, err *error) { *err = tx.CreateStore([]byte("store")) },
				func(tx engine.Transaction, err *error) { _, *err = tx.GetStore([]byte("store")) },
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				ng, cleanup := builder()
				defer cleanup()
				defer func() {
					require.NoError(t, ng.Close())
				}()

				tx, err := ng.Begin(context.Background(), engine.TxOptions{
					Writable: true,
				})
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
		defer func() {
			require.NoError(t, ng.Close())
		}()

		tx, err := ng.Begin(context.Background(), engine.TxOptions{
			Writable: true,
		})
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.CreateStore([]byte("store"))
		require.NoError(t, err)

		st, err := tx.GetStore([]byte("store"))
		require.NoError(t, err)
		require.NotNil(t, st)
	})

	t.Run("Should fail if store already exists", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()
		defer func() {
			require.NoError(t, ng.Close())
		}()

		tx, err := ng.Begin(context.Background(), engine.TxOptions{
			Writable: true,
		})
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.CreateStore([]byte("store"))
		require.NoError(t, err)
		err = tx.CreateStore([]byte("store"))
		require.Equal(t, engine.ErrStoreAlreadyExists, err)
	})

	t.Run("Should fail if context canceled", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()
		defer func() {
			require.NoError(t, ng.Close())
		}()

		ctx, cancel := context.WithCancel(context.Background())
		tx, err := ng.Begin(ctx, engine.TxOptions{
			Writable: true,
		})
		require.NoError(t, err)
		defer tx.Rollback()

		cancel()
		err = tx.CreateStore([]byte("store"))
		require.Equal(t, context.Canceled, err)
	})
}

// TestTransactionGetStore verifies GetStore behaviour.
func TestTransactionGetStore(t *testing.T, builder Builder) {
	t.Run("Should fail if store not found", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()
		defer func() {
			require.NoError(t, ng.Close())
		}()

		tx, err := ng.Begin(context.Background(), engine.TxOptions{
			Writable: false,
		})
		require.NoError(t, err)
		defer tx.Rollback()

		_, err = tx.GetStore([]byte("store"))
		require.Equal(t, engine.ErrStoreNotFound, err)
	})

	t.Run("Should return the right store", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()
		defer func() {
			require.NoError(t, ng.Close())
		}()

		tx, err := ng.Begin(context.Background(), engine.TxOptions{
			Writable: true,
		})
		require.NoError(t, err)
		defer tx.Rollback()

		// create two stores
		err = tx.CreateStore([]byte("storea"))
		require.NoError(t, err)

		err = tx.CreateStore([]byte("storeb"))
		require.NoError(t, err)

		// fetch first store
		sta, err := tx.GetStore([]byte("storea"))
		require.NoError(t, err)

		// fetch second store
		stb, err := tx.GetStore([]byte("storeb"))
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

	t.Run("Should fail if context canceled", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()
		defer func() {
			require.NoError(t, ng.Close())
		}()

		ctx, cancel := context.WithCancel(context.Background())
		tx, err := ng.Begin(ctx, engine.TxOptions{
			Writable: true,
		})
		require.NoError(t, err)
		defer tx.Rollback()

		// create two stores
		err = tx.CreateStore([]byte("store"))
		require.NoError(t, err)

		cancel()

		_, err = tx.GetStore([]byte("store"))
		require.Equal(t, context.Canceled, err)
	})
}

// TestTransactionDropStore verifies DropStore behaviour.
func TestTransactionDropStore(t *testing.T, builder Builder) {
	t.Run("Should drop a store", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()
		defer func() {
			require.NoError(t, ng.Close())
		}()

		tx, err := ng.Begin(context.Background(), engine.TxOptions{
			Writable: true,
		})
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.CreateStore([]byte("store"))
		require.NoError(t, err)

		err = tx.DropStore([]byte("store"))
		require.NoError(t, err)

		_, err = tx.GetStore([]byte("store"))
		require.Equal(t, engine.ErrStoreNotFound, err)
	})

	t.Run("Should fail if store not found", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()
		defer func() {
			require.NoError(t, ng.Close())
		}()

		tx, err := ng.Begin(context.Background(), engine.TxOptions{
			Writable: true,
		})
		require.NoError(t, err)
		defer tx.Rollback()

		err = tx.DropStore([]byte("store"))
		require.Equal(t, engine.ErrStoreNotFound, err)
	})

	t.Run("Should fail if context canceled", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()
		defer func() {
			require.NoError(t, ng.Close())
		}()

		ctx, cancel := context.WithCancel(context.Background())
		tx, err := ng.Begin(ctx, engine.TxOptions{
			Writable: true,
		})
		require.NoError(t, err)
		defer tx.Rollback()

		// create two stores
		err = tx.CreateStore([]byte("store"))
		require.NoError(t, err)

		cancel()

		err = tx.DropStore([]byte("store"))
		require.Equal(t, context.Canceled, err)
	})
}

func storeBuilder(t testing.TB, builder Builder) (engine.Store, func()) {
	return storeBuilderWithContext(context.Background(), t, builder)
}

func storeBuilderWithContext(ctx context.Context, t testing.TB, builder Builder) (engine.Store, func()) {
	ng, cleanup := builder()
	tx, err := ng.Begin(ctx, engine.TxOptions{
		Writable: true,
	})
	require.NoError(t, err)
	err = tx.CreateStore([]byte("test"))
	require.NoError(t, err)
	st, err := tx.GetStore([]byte("test"))
	require.NoError(t, err)
	return st, func() {
		defer cleanup()
		defer func() {
			require.NoError(t, ng.Close())
		}()
		defer tx.Rollback()
	}
}

// TestStoreIterator verifies Iterator behaviour.
func TestStoreIterator(t *testing.T, builder Builder) {
	t.Run("Should not fail with no documents", func(t *testing.T) {
		fn := func(t *testing.T, reverse bool) {
			st, cleanup := storeBuilder(t, builder)
			defer cleanup()

			it := st.Iterator(engine.IteratorOptions{Reverse: reverse})
			defer it.Close()
			i := 0

			for it.Seek(nil); it.Valid(); it.Next() {
				i++
			}
			require.NoError(t, it.Err())
			require.Zero(t, i)
		}
		t.Run("Reverse: false", func(t *testing.T) {
			fn(t, false)
		})
		t.Run("Reverse: true", func(t *testing.T) {
			fn(t, true)
		})
	})

	t.Run("Should stop the iteration if context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		st, cleanup := storeBuilderWithContext(ctx, t, builder)
		defer cleanup()

		for i := 1; i <= 10; i++ {
			err := st.Put([]byte{uint8(i)}, []byte{uint8(i + 20)})
			require.NoError(t, err)
		}

		it := st.Iterator(engine.IteratorOptions{})
		defer it.Close()

		cancel()

		var i int
		for it.Seek(nil); it.Valid(); it.Next() {
			i++
		}
		require.Equal(t, context.Canceled, it.Err())
		require.Zero(t, i)
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
		it := st.Iterator(engine.IteratorOptions{})
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
		require.NoError(t, it.Err())

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
		it := st.Iterator(engine.IteratorOptions{Reverse: true})
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
		require.NoError(t, it.Err())
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
		it := st.Iterator(engine.IteratorOptions{})
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
		require.NoError(t, it.Err())
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
		it := st.Iterator(engine.IteratorOptions{Reverse: true})
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
		require.NoError(t, it.Err())
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
		it := st.Iterator(engine.IteratorOptions{})
		defer it.Close()

		for it.Seek([]byte{2}); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			v, _ := item.ValueCopy(nil)
			require.Equal(t, []byte{3}, k)
			require.Equal(t, []byte{3}, v)
			called = true
		}
		require.NoError(t, it.Err())

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
		it := st.Iterator(engine.IteratorOptions{Reverse: true})
		defer it.Close()

		for it.Seek([]byte{2}); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			v, _ := item.ValueCopy(nil)
			require.Equal(t, []byte{1}, k)
			require.Equal(t, []byte{1}, v)
			called = true
		}
		require.NoError(t, it.Err())
		require.True(t, called)
	})

	t.Run("With reverse true, one key in the store, and no pivot, should return that key", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		k := []byte{0xFF, 0xFF, 0xFF, 0xFF}
		err := st.Put(k, []byte{1})
		require.NoError(t, err)

		it := st.Iterator(engine.IteratorOptions{Reverse: true})
		defer it.Close()

		it.Seek(nil)

		require.NoError(t, it.Err())
		require.True(t, it.Valid())
		require.Equal(t, it.Item().Key(), k)
	})

	t.Run("Iterating while deleting current key should work", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		for i := 0; i < 50; i++ {
			err := st.Put([]byte{byte(i)}, []byte{byte(i)})
			require.NoError(t, err)
		}

		i := 0
		it := st.Iterator(engine.IteratorOptions{})
		defer it.Close()

		for it.Seek(nil); it.Valid() && i < 50; it.Next() {
			require.Equal(t, []byte{byte(i)}, it.Item().Key())

			err := st.Delete([]byte{byte(i)})
			require.NoError(t, err)
			i++
		}
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

	t.Run("Should fail when value is nil or empty", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		err := st.Put([]byte("foo"), nil)
		require.Error(t, err)

		err = st.Put([]byte("foo"), []byte(""))
		require.Error(t, err)
	})

	t.Run("Should fail if context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		st, cleanup := storeBuilderWithContext(ctx, t, builder)
		defer cleanup()

		cancel()
		err := st.Put([]byte("foo"), []byte("FOO"))
		require.Equal(t, context.Canceled, err)
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

	t.Run("Should fail if context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		st, cleanup := storeBuilderWithContext(ctx, t, builder)
		defer cleanup()

		err := st.Put([]byte("foo"), []byte("FOO"))
		require.NoError(t, err)

		cancel()
		_, err = st.Get([]byte("foo"))
		require.Equal(t, context.Canceled, err)
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
		err = st.Delete([]byte("bar"))
		require.NoError(t, err)

		// try again, should fail
		err = st.Delete([]byte("bar"))
		require.Equal(t, engine.ErrKeyNotFound, err)

		// make sure it didn't also delete the other one
		v, err = st.Get([]byte("foo"))
		require.NoError(t, err)
		require.Equal(t, []byte("FOO"), v)

		// the deleted key must not appear on iteration
		it := st.Iterator(engine.IteratorOptions{})
		defer it.Close()
		i := 0
		for it.Seek(nil); it.Valid(); it.Next() {
			require.Equal(t, []byte("foo"), it.Item().Key())
			i++
		}
		require.Equal(t, 1, i)
	})

	t.Run("Should not rollback document if deleted then put", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()

		tx, err := ng.Begin(context.Background(), engine.TxOptions{Writable: true})
		require.NoError(t, err)
		err = tx.CreateStore([]byte("test"))
		require.NoError(t, err)
		st, err := tx.GetStore([]byte("test"))
		require.NoError(t, err)

		err = st.Put([]byte("foo"), []byte("FOO"))
		require.NoError(t, err)

		// delete the key
		err = st.Delete([]byte("foo"))
		require.NoError(t, err)

		v, err := st.Get([]byte("foo"))
		require.Equal(t, engine.ErrKeyNotFound, err)

		err = st.Put([]byte("foo"), []byte("bar"))
		require.NoError(t, err)

		v, err = st.Get([]byte("foo"))
		require.Equal(t, []byte("bar"), v)

		// commit and reopen a transaction
		err = tx.Commit()
		require.NoError(t, err)

		tx, err = ng.Begin(context.Background(), engine.TxOptions{Writable: false})
		require.NoError(t, err)
		defer tx.Rollback()

		st, err = tx.GetStore([]byte("test"))
		require.NoError(t, err)

		v, err = st.Get([]byte("foo"))
		require.Equal(t, []byte("bar"), v)
	})

	t.Run("Should fail if context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		st, cleanup := storeBuilderWithContext(ctx, t, builder)
		defer cleanup()

		err := st.Put([]byte("foo"), []byte("FOO"))
		require.NoError(t, err)

		cancel()
		err = st.Delete([]byte("foo"))
		require.Equal(t, context.Canceled, err)
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

		it := st.Iterator(engine.IteratorOptions{})
		defer it.Close()
		it.Seek(nil)
		require.NoError(t, it.Err())
		require.False(t, it.Valid())
	})

	t.Run("Should fail if context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		st, cleanup := storeBuilderWithContext(ctx, t, builder)
		defer cleanup()

		err := st.Put([]byte("foo"), []byte("FOO"))
		require.NoError(t, err)

		cancel()
		err = st.Truncate()
		require.Equal(t, context.Canceled, err)
	})
}

// TestStoreNextSequence verifies NextSequence behaviour.
func TestStoreNextSequence(t *testing.T, builder Builder) {
	t.Run("Should fail if tx not writable", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()
		defer func() {
			require.NoError(t, ng.Close())
		}()

		tx, err := ng.Begin(context.Background(), engine.TxOptions{
			Writable: true,
		})

		require.NoError(t, err)
		err = tx.CreateStore([]byte("test"))
		require.NoError(t, err)
		err = tx.Commit()
		require.NoError(t, err)

		tx, err = ng.Begin(context.Background(), engine.TxOptions{
			Writable: false,
		})
		require.NoError(t, err)
		defer tx.Rollback()

		st, err := tx.GetStore([]byte("test"))
		require.NoError(t, err)

		_, err = st.NextSequence()
		require.Error(t, err)
	})

	t.Run("Should return the next sequence", func(t *testing.T) {
		st, cleanup := storeBuilder(t, builder)
		defer cleanup()

		for i := uint64(1); i < 100; i++ {
			s, err := st.NextSequence()
			require.NoError(t, err)
			require.Equal(t, i, s)
		}
	})

	t.Run("Should store the last sequence", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()
		defer func() {
			require.NoError(t, ng.Close())
		}()

		tx, err := ng.Begin(context.Background(), engine.TxOptions{
			Writable: true,
		})

		require.NoError(t, err)
		err = tx.CreateStore([]byte("test"))
		require.NoError(t, err)
		st, err := tx.GetStore([]byte("test"))
		require.NoError(t, err)

		s1, err := st.NextSequence()
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		tx, err = ng.Begin(context.Background(), engine.TxOptions{
			Writable: true,
		})
		require.NoError(t, err)
		defer tx.Rollback()

		st, err = tx.GetStore([]byte("test"))
		require.NoError(t, err)
		s2, err := st.NextSequence()
		require.NoError(t, err)
		require.Equal(t, s1+1, s2)
	})

	t.Run("Should fail if context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		st, cleanup := storeBuilderWithContext(ctx, t, builder)
		defer cleanup()

		cancel()
		_, err := st.NextSequence()
		require.Equal(t, context.Canceled, err)
	})
}

// TestQueries test simple queries against the engine.
func TestQueries(t *testing.T, builder Builder) {
	t.Run("SELECT", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()
		defer func() {
			require.NoError(t, ng.Close())
		}()

		db, err := genji.New(context.Background(), ng)
		require.NoError(t, err)

		d, err := db.QueryDocument(`
			CREATE TABLE test;
			INSERT INTO test (a) VALUES (1), (2), (3), (4);
			SELECT COUNT(*) FROM test;
		`)
		require.NoError(t, err)
		var count int
		err = document.Scan(d, &count)
		require.NoError(t, err)
		require.Equal(t, 4, count)

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
		defer func() {
			require.NoError(t, ng.Close())
		}()

		db, err := genji.New(context.Background(), ng)
		require.NoError(t, err)

		err = db.Exec(`
			CREATE TABLE test;
			INSERT INTO test (a) VALUES (1), (2), (3), (4);
		`)
		require.NoError(t, err)
	})

	t.Run("UPDATE", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()
		defer func() {
			require.NoError(t, ng.Close())
		}()

		db, err := genji.New(context.Background(), ng)
		require.NoError(t, err)

		st, err := db.Query(`
				CREATE TABLE test;
				INSERT INTO test (a) VALUES (1), (2), (3), (4);
				UPDATE test SET a = 5;
				SELECT * FROM test;
			`)
		require.NoError(t, err)
		defer st.Close()
		var buf bytes.Buffer
		err = testutil.IteratorToJSONArray(&buf, st)
		require.NoError(t, err)
		require.JSONEq(t, `[{"a": 5},{"a": 5},{"a": 5},{"a": 5}]`, buf.String())
	})

	t.Run("DELETE", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()
		defer func() {
			require.NoError(t, ng.Close())
		}()

		db, err := genji.New(context.Background(), ng)
		require.NoError(t, err)

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

		d, err := db.QueryDocument(`
			DELETE FROM test WHERE a > 2;
			SELECT COUNT(*) FROM test;
		`)
		require.NoError(t, err)
		var count int
		err = document.Scan(d, &count)
		require.NoError(t, err)
		require.Equal(t, 2, count)
	})
}

// TestQueriesSameTransaction test simple queries in the same transaction.
func TestQueriesSameTransaction(t *testing.T, builder Builder) {
	t.Run("SELECT", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()
		defer func() {
			require.NoError(t, ng.Close())
		}()

		db, err := genji.New(context.Background(), ng)
		require.NoError(t, err)

		err = db.Update(func(tx *genji.Tx) error {
			d, err := tx.QueryDocument(`
				CREATE TABLE test;
				INSERT INTO test (a) VALUES (1), (2), (3), (4);
				SELECT COUNT(*) FROM test;
			`)
			require.NoError(t, err)
			var count int
			err = document.Scan(d, &count)
			require.NoError(t, err)
			require.Equal(t, 4, count)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("INSERT", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()
		defer func() {
			require.NoError(t, ng.Close())
		}()

		db, err := genji.New(context.Background(), ng)
		require.NoError(t, err)

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
		defer func() {
			require.NoError(t, ng.Close())
		}()

		db, err := genji.New(context.Background(), ng)
		require.NoError(t, err)

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
			err = testutil.IteratorToJSONArray(&buf, st)
			require.NoError(t, err)
			require.JSONEq(t, `[{"a": 5},{"a": 5},{"a": 5},{"a": 5}]`, buf.String())
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("DELETE", func(t *testing.T) {
		ng, cleanup := builder()
		defer cleanup()
		defer func() {
			require.NoError(t, ng.Close())
		}()

		db, err := genji.New(context.Background(), ng)
		require.NoError(t, err)

		err = db.Update(func(tx *genji.Tx) error {
			d, err := tx.QueryDocument(`
			CREATE TABLE test;
			INSERT INTO test (a) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
			DELETE FROM test WHERE a > 2;
			SELECT COUNT(*) FROM test;
		`)
			require.NoError(t, err)
			var count int
			document.Scan(d, &count)
			require.NoError(t, err)
			require.Equal(t, 2, count)
			return nil
		})
		require.NoError(t, err)
	})
}
