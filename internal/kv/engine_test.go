package kv_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/kv"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func builder(t testing.TB) *kv.Engine {
	dir := tempDir(t)

	ng, err := kv.NewEngine(filepath.Join(dir, "pebble"), nil)
	assert.NoError(t, err)

	return ng
}

// TestEngine runs a list of tests against the provided kv.
func TestEngine(t *testing.T) {
	t.Run("Close", func(t *testing.T) {
		ng := builder(t)

		assert.NoError(t, ng.Close())
	})
}

func getValue(t *testing.T, st *kv.Store, key []byte) []byte {
	v, err := st.Get([]byte(key))
	assert.NoError(t, err)
	return v
}

// TestTransactionCommitRollback runs a list of tests to verify Commit and Rollback
// behaviour of transactions created from the given kv.
func TestTransactionCommitRollback(t *testing.T) {
	ng := builder(t)
	defer func() {
		assert.NoError(t, ng.Close())
	}()

	t.Run("Commit on read-only transaction should fail", func(t *testing.T) {
		tx, err := ng.Begin(context.Background(), kv.TxOptions{
			Writable: false,
		})
		assert.NoError(t, err)
		defer tx.Rollback()

		err = tx.Commit()
		assert.Error(t, err)
	})

	t.Run("Commit after rollback should fail", func(t *testing.T) {
		tx, err := ng.Begin(context.Background(), kv.TxOptions{
			Writable: true,
		})
		assert.NoError(t, err)
		defer tx.Rollback()

		err = tx.Rollback()
		assert.NoError(t, err)

		err = tx.Commit()
		assert.Error(t, err)
	})

	t.Run("Commit after context canceled should fail", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		tx, err := ng.Begin(ctx, kv.TxOptions{
			Writable: true,
		})
		assert.NoError(t, err)

		assert.NoError(t, tx.CreateStore([]byte("test")))
		st, err := tx.GetStore([]byte("test"))
		assert.NoError(t, err)
		err = st.Put([]byte("a"), []byte("b"))
		assert.NoError(t, err)

		cancel()

		err = tx.Commit()
		assert.Error(t, err)

		// ensure data has not been persisted
		tx, err = ng.Begin(context.Background(), kv.TxOptions{
			Writable: false,
		})
		assert.NoError(t, err)
		defer tx.Rollback()

		_, err = tx.GetStore([]byte("test"))
		assert.Error(t, err)
	})

	t.Run("Rollback after commit should return ErrTransactionDiscarded", func(t *testing.T) {
		tx, err := ng.Begin(context.Background(), kv.TxOptions{
			Writable: true,
		})
		assert.NoError(t, err)
		defer tx.Rollback()

		err = tx.Commit()
		assert.NoError(t, err)

		err = tx.Rollback()
		assert.ErrorIs(t, err, kv.ErrTransactionDiscarded)
	})

	t.Run("Commit after commit should return ErrTransactionDiscarded", func(t *testing.T) {
		tx, err := ng.Begin(context.Background(), kv.TxOptions{
			Writable: true,
		})
		assert.NoError(t, err)
		defer tx.Rollback()

		err = tx.Commit()
		assert.NoError(t, err)

		err = tx.Commit()
		assert.ErrorIs(t, err, kv.ErrTransactionDiscarded)
	})

	t.Run("Rollback after rollback should should return ErrTransactionDiscarded", func(t *testing.T) {
		tx, err := ng.Begin(context.Background(), kv.TxOptions{
			Writable: false,
		})
		assert.NoError(t, err)
		defer tx.Rollback()

		err = tx.Rollback()
		assert.NoError(t, err)

		err = tx.Rollback()
		assert.ErrorIs(t, err, kv.ErrTransactionDiscarded)
	})

	t.Run("Rollback after context canceled should return context.Canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		tx, err := ng.Begin(ctx, kv.TxOptions{
			Writable: true,
		})
		assert.NoError(t, err)

		cancel()

		err = tx.Rollback()
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("Read-Only write attempts", func(t *testing.T) {
		tx, err := ng.Begin(context.Background(), kv.TxOptions{
			Writable: true,
		})
		assert.NoError(t, err)

		// create store for testing store methods
		err = tx.CreateStore([]byte("store1"))
		assert.NoError(t, err)

		err = tx.Commit()
		assert.NoError(t, err)

		// create a new read-only transaction
		tx, err = ng.Begin(context.Background(), kv.TxOptions{
			Writable: false,
		})
		assert.NoError(t, err)
		defer tx.Rollback()

		// fetch the store and the index
		st, err := tx.GetStore([]byte("store1"))
		assert.NoError(t, err)

		tests := []struct {
			name string
			err  error
			fn   func(*error)
		}{
			{"CreateStore", kv.ErrTransactionReadOnly, func(err *error) { *err = tx.CreateStore([]byte("store")) }},
			{"DropStore", kv.ErrTransactionReadOnly, func(err *error) { *err = tx.DropStore([]byte("store")) }},
			{"StorePut", kv.ErrTransactionReadOnly, func(err *error) { *err = st.Put([]byte("id"), nil) }},
			{"StoreDelete", kv.ErrTransactionReadOnly, func(err *error) { *err = st.Delete([]byte("id")) }},
			{"StoreTruncate", kv.ErrTransactionReadOnly, func(err *error) { *err = st.Truncate() }},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				var err error
				test.fn(&err)

				assert.ErrorIs(t, err, test.err)
			})
		}
	})

	t.Run("Commit / Rollback data persistence", func(t *testing.T) {
		// this test checks if rollback undoes data changes correctly and if commit keeps data correctly
		tests := []struct {
			name    string
			initFn  func(*kv.Transaction) error
			writeFn func(*kv.Transaction, *error)
			readFn  func(*kv.Transaction, *error)
		}{
			{
				"CreateStore",
				nil,
				func(tx *kv.Transaction, err *error) { *err = tx.CreateStore([]byte("store")) },
				func(tx *kv.Transaction, err *error) { _, *err = tx.GetStore([]byte("store")) },
			},
			{
				"DropStore",
				func(tx *kv.Transaction) error { return tx.CreateStore([]byte("store")) },
				func(tx *kv.Transaction, err *error) { *err = tx.DropStore([]byte("store")) },
				func(tx *kv.Transaction, err *error) { *err = tx.CreateStore([]byte("store")) },
			},
			{
				"StorePut",
				func(tx *kv.Transaction) error { return tx.CreateStore([]byte("store")) },
				func(tx *kv.Transaction, err *error) {
					st, er := tx.GetStore([]byte("store"))
					assert.NoError(t, er)
					assert.NoError(t, st.Put([]byte("foo"), []byte("FOO")))
				},
				func(tx *kv.Transaction, err *error) {
					st, er := tx.GetStore([]byte("store"))
					assert.NoError(t, er)
					_, *err = st.Get([]byte("foo"))
				},
			},
		}

		for _, test := range tests {
			t.Run(test.name+"/rollback", func(t *testing.T) {
				ng := builder(t)
				defer func() {
					assert.NoError(t, ng.Close())
				}()

				if test.initFn != nil {
					func() {
						tx, err := ng.Begin(context.Background(), kv.TxOptions{
							Writable: true,
						})
						assert.NoError(t, err)
						defer tx.Rollback()

						err = test.initFn(tx)
						assert.NoError(t, err)
						err = tx.Commit()
						assert.NoError(t, err)
					}()
				}

				tx, err := ng.Begin(context.Background(), kv.TxOptions{
					Writable: true,
				})
				assert.NoError(t, err)
				defer tx.Rollback()

				test.writeFn(tx, &err)
				assert.NoError(t, err)

				err = tx.Rollback()
				assert.NoError(t, err)

				tx, err = ng.Begin(context.Background(), kv.TxOptions{
					Writable: true,
				})
				assert.NoError(t, err)
				defer tx.Rollback()

				test.readFn(tx, &err)
				assert.Error(t, err)
			})
		}

		for _, test := range tests {
			ng := builder(t)
			defer func() {
				assert.NoError(t, ng.Close())
			}()

			t.Run(test.name+"/commit", func(t *testing.T) {
				if test.initFn != nil {
					func() {
						tx, err := ng.Begin(context.Background(), kv.TxOptions{
							Writable: true,
						})
						assert.NoError(t, err)
						defer tx.Rollback()

						err = test.initFn(tx)
						assert.NoError(t, err)
						err = tx.Commit()
						assert.NoError(t, err)
					}()
				}

				tx, err := ng.Begin(context.Background(), kv.TxOptions{
					Writable: true,
				})
				assert.NoError(t, err)
				defer tx.Rollback()

				test.writeFn(tx, &err)
				assert.NoError(t, err)

				err = tx.Commit()
				assert.NoError(t, err)

				tx, err = ng.Begin(context.Background(), kv.TxOptions{
					Writable: true,
				})
				assert.NoError(t, err)
				defer tx.Rollback()

				test.readFn(tx, &err)
				assert.NoError(t, err)
			})
		}
	})

	t.Run("Data should be visible within the same transaction", func(t *testing.T) {
		tests := []struct {
			name    string
			writeFn func(*kv.Transaction, *error)
			readFn  func(*kv.Transaction, *error)
		}{
			{
				"CreateStore",
				func(tx *kv.Transaction, err *error) { *err = tx.CreateStore([]byte("store")) },
				func(tx *kv.Transaction, err *error) { _, *err = tx.GetStore([]byte("store")) },
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				ng := builder(t)
				defer func() {
					assert.NoError(t, ng.Close())
				}()

				tx, err := ng.Begin(context.Background(), kv.TxOptions{
					Writable: true,
				})
				assert.NoError(t, err)
				defer tx.Rollback()

				test.writeFn(tx, &err)
				assert.NoError(t, err)

				test.readFn(tx, &err)
				assert.NoError(t, err)
			})
		}
	})
}

// TestTransactionCreateStore verifies CreateStore behaviour.
func TestTransactionCreateStore(t *testing.T) {
	t.Run("Should create a store", func(t *testing.T) {
		ng := builder(t)
		defer func() {
			assert.NoError(t, ng.Close())
		}()

		tx, err := ng.Begin(context.Background(), kv.TxOptions{
			Writable: true,
		})
		assert.NoError(t, err)
		defer tx.Rollback()

		err = tx.CreateStore([]byte("store"))
		assert.NoError(t, err)

		st, err := tx.GetStore([]byte("store"))
		assert.NoError(t, err)
		require.NotNil(t, st)
	})

	t.Run("Should fail if store already exists", func(t *testing.T) {
		ng := builder(t)
		defer func() {
			assert.NoError(t, ng.Close())
		}()

		tx, err := ng.Begin(context.Background(), kv.TxOptions{
			Writable: true,
		})
		assert.NoError(t, err)
		defer tx.Rollback()

		err = tx.CreateStore([]byte("store"))
		assert.NoError(t, err)
		err = tx.CreateStore([]byte("store"))
		assert.ErrorIs(t, err, kv.ErrStoreAlreadyExists)
	})

	t.Run("Should fail if context canceled", func(t *testing.T) {
		ng := builder(t)
		defer func() {
			assert.NoError(t, ng.Close())
		}()

		ctx, cancel := context.WithCancel(context.Background())
		tx, err := ng.Begin(ctx, kv.TxOptions{
			Writable: true,
		})
		assert.NoError(t, err)
		defer tx.Rollback()

		cancel()
		err = tx.CreateStore([]byte("store"))
		assert.ErrorIs(t, err, context.Canceled)
	})
}

// TestTransactionGetStore verifies GetStore behaviour.
func TestTransactionGetStore(t *testing.T) {
	t.Run("Should fail if store not found", func(t *testing.T) {
		ng := builder(t)
		defer func() {
			assert.NoError(t, ng.Close())
		}()

		tx, err := ng.Begin(context.Background(), kv.TxOptions{
			Writable: false,
		})
		assert.NoError(t, err)
		defer tx.Rollback()

		_, err = tx.GetStore([]byte("store"))
		assert.ErrorIs(t, err, kv.ErrStoreNotFound)
	})

	t.Run("Should return the right store", func(t *testing.T) {
		ng := builder(t)
		defer func() {
			assert.NoError(t, ng.Close())
		}()

		tx, err := ng.Begin(context.Background(), kv.TxOptions{
			Writable: true,
		})
		assert.NoError(t, err)
		defer tx.Rollback()

		// create two stores
		err = tx.CreateStore([]byte("storea"))
		assert.NoError(t, err)

		err = tx.CreateStore([]byte("storeb"))
		assert.NoError(t, err)

		// fetch first store
		sta, err := tx.GetStore([]byte("storea"))
		assert.NoError(t, err)

		// fetch second store
		stb, err := tx.GetStore([]byte("storeb"))
		assert.NoError(t, err)

		// insert data in first store
		err = sta.Put([]byte("foo"), []byte("FOO"))
		assert.NoError(t, err)

		// use sta to fetch data and verify if it's present
		v := getValue(t, sta, []byte("foo"))
		require.Equal(t, v, []byte("FOO"))

		// use stb to fetch data and verify it's not present
		_, err = stb.Get([]byte("foo"))
		assert.ErrorIs(t, err, kv.ErrKeyNotFound)
	})

	t.Run("Should fail if context canceled", func(t *testing.T) {
		ng := builder(t)
		defer func() {
			assert.NoError(t, ng.Close())
		}()

		ctx, cancel := context.WithCancel(context.Background())
		tx, err := ng.Begin(ctx, kv.TxOptions{
			Writable: true,
		})
		assert.NoError(t, err)
		defer tx.Rollback()

		// create two stores
		err = tx.CreateStore([]byte("store"))
		assert.NoError(t, err)

		cancel()

		_, err = tx.GetStore([]byte("store"))
		assert.ErrorIs(t, err, context.Canceled)
	})
}

// TestTransactionDropStore verifies DropStore behaviour.
func TestTransactionDropStore(t *testing.T) {
	t.Run("Should drop a store", func(t *testing.T) {
		ng := builder(t)
		defer func() {
			assert.NoError(t, ng.Close())
		}()

		tx, err := ng.Begin(context.Background(), kv.TxOptions{
			Writable: true,
		})
		assert.NoError(t, err)
		defer tx.Rollback()

		err = tx.CreateStore([]byte("store"))
		assert.NoError(t, err)

		err = tx.DropStore([]byte("store"))
		assert.NoError(t, err)

		_, err = tx.GetStore([]byte("store"))
		assert.ErrorIs(t, err, kv.ErrStoreNotFound)
	})

	t.Run("Should fail if store not found", func(t *testing.T) {
		ng := builder(t)
		defer func() {
			assert.NoError(t, ng.Close())
		}()

		tx, err := ng.Begin(context.Background(), kv.TxOptions{
			Writable: true,
		})
		assert.NoError(t, err)
		defer tx.Rollback()

		err = tx.DropStore([]byte("store"))
		assert.ErrorIs(t, err, kv.ErrStoreNotFound)
	})

	t.Run("Should fail if context canceled", func(t *testing.T) {
		ng := builder(t)
		defer func() {
			assert.NoError(t, ng.Close())
		}()

		ctx, cancel := context.WithCancel(context.Background())
		tx, err := ng.Begin(ctx, kv.TxOptions{
			Writable: true,
		})
		assert.NoError(t, err)
		defer tx.Rollback()

		// create two stores
		err = tx.CreateStore([]byte("store"))
		assert.NoError(t, err)

		cancel()

		err = tx.DropStore([]byte("store"))
		assert.ErrorIs(t, err, context.Canceled)
	})
}

func storeBuilder(t testing.TB) (*kv.Store, func()) {
	return storeBuilderWithContext(context.Background(), t)
}

func storeBuilderWithContext(ctx context.Context, t testing.TB) (*kv.Store, func()) {
	ng := builder(t)
	tx, err := ng.Begin(ctx, kv.TxOptions{
		Writable: true,
	})
	assert.NoError(t, err)
	err = tx.CreateStore([]byte("test"))
	assert.NoError(t, err)
	st, err := tx.GetStore([]byte("test"))
	assert.NoError(t, err)
	return st, func() {
		defer func() {
			assert.NoError(t, ng.Close())
		}()
		defer tx.Rollback()
	}
}

// TestStorePut verifies Put behaviour.
func TestStorePut(t *testing.T) {
	t.Run("Should insert data", func(t *testing.T) {
		st, cleanup := storeBuilder(t)
		defer cleanup()

		err := st.Put([]byte("foo"), []byte("FOO"))
		assert.NoError(t, err)

		v := getValue(t, st, []byte("foo"))
		require.Equal(t, []byte("FOO"), v)
	})

	t.Run("Should replace existing key", func(t *testing.T) {
		st, cleanup := storeBuilder(t)
		defer cleanup()

		err := st.Put([]byte("foo"), []byte("FOO"))
		assert.NoError(t, err)

		err = st.Put([]byte("foo"), []byte("BAR"))
		assert.NoError(t, err)

		v := getValue(t, st, []byte("foo"))
		require.Equal(t, []byte("BAR"), v)
	})

	t.Run("Should fail when key is nil or empty", func(t *testing.T) {
		st, cleanup := storeBuilder(t)
		defer cleanup()

		err := st.Put(nil, []byte("FOO"))
		assert.Error(t, err)

		err = st.Put([]byte(""), []byte("BAR"))
		assert.Error(t, err)
	})

	t.Run("Should fail when value is nil or empty", func(t *testing.T) {
		st, cleanup := storeBuilder(t)
		defer cleanup()

		err := st.Put([]byte("foo"), nil)
		assert.Error(t, err)

		err = st.Put([]byte("foo"), []byte(""))
		assert.Error(t, err)
	})

	t.Run("Should fail if context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		st, cleanup := storeBuilderWithContext(ctx, t)
		defer cleanup()

		cancel()
		err := st.Put([]byte("foo"), []byte("FOO"))
		assert.ErrorIs(t, err, context.Canceled)
	})
}

// TestStoreGet verifies Get behaviour.
func TestStoreGet(t *testing.T) {
	t.Run("Should fail if not found", func(t *testing.T) {
		st, cleanup := storeBuilder(t)
		defer cleanup()

		r, err := st.Get([]byte("id"))
		assert.ErrorIs(t, err, kv.ErrKeyNotFound)
		require.Nil(t, r)
	})

	t.Run("Should return the right key", func(t *testing.T) {
		st, cleanup := storeBuilder(t)
		defer cleanup()

		err := st.Put([]byte("foo"), []byte("FOO"))
		assert.NoError(t, err)
		err = st.Put([]byte("bar"), []byte("BAR"))
		assert.NoError(t, err)

		v := getValue(t, st, []byte("foo"))
		require.Equal(t, []byte("FOO"), v)

		v = getValue(t, st, []byte("bar"))
		require.Equal(t, []byte("BAR"), v)
	})

	t.Run("Should fail if context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		st, cleanup := storeBuilderWithContext(ctx, t)
		defer cleanup()

		err := st.Put([]byte("foo"), []byte("FOO"))
		assert.NoError(t, err)

		cancel()
		_, err = st.Get([]byte("foo"))
		assert.ErrorIs(t, err, context.Canceled)
	})
}

// TestStoreDelete verifies Delete behaviour.
func TestStoreDelete(t *testing.T) {
	t.Run("Should fail if not found", func(t *testing.T) {
		st, cleanup := storeBuilder(t)
		defer cleanup()

		err := st.Delete([]byte("id"))
		assert.ErrorIs(t, err, kv.ErrKeyNotFound)
	})

	t.Run("Should delete the right document", func(t *testing.T) {
		st, cleanup := storeBuilder(t)
		defer cleanup()

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
		err = st.Delete([]byte("bar"))
		assert.ErrorIs(t, err, kv.ErrKeyNotFound)

		// make sure it didn't also delete the other one
		v = getValue(t, st, []byte("foo"))
		require.Equal(t, []byte("FOO"), v)

		// the deleted key must not appear on iteration
		it := st.Iterator(nil)
		defer it.Close()
		i := 0
		for it.First(); it.Valid(); it.Next() {
			require.Equal(t, []byte("foo"), kv.TrimPrefix(it.Key(), st.Prefix))
			i++
		}
		require.Equal(t, 1, i)
	})

	t.Run("Should not rollback document if deleted then put", func(t *testing.T) {
		ng := builder(t)

		tx, err := ng.Begin(context.Background(), kv.TxOptions{Writable: true})
		assert.NoError(t, err)
		err = tx.CreateStore([]byte("test"))
		assert.NoError(t, err)
		st, err := tx.GetStore([]byte("test"))
		assert.NoError(t, err)

		err = st.Put([]byte("foo"), []byte("FOO"))
		assert.NoError(t, err)

		// delete the key
		err = st.Delete([]byte("foo"))
		assert.NoError(t, err)

		_, err = st.Get([]byte("foo"))
		assert.ErrorIs(t, err, kv.ErrKeyNotFound)

		err = st.Put([]byte("foo"), []byte("bar"))
		assert.NoError(t, err)

		v := getValue(t, st, []byte("foo"))
		require.Equal(t, []byte("bar"), v)

		// commit and reopen a transaction
		err = tx.Commit()
		assert.NoError(t, err)

		tx, err = ng.Begin(context.Background(), kv.TxOptions{Writable: false})
		assert.NoError(t, err)
		defer tx.Rollback()

		st, err = tx.GetStore([]byte("test"))
		assert.NoError(t, err)

		v = getValue(t, st, []byte("foo"))
		require.Equal(t, []byte("bar"), v)
	})

	t.Run("Should fail if context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		st, cleanup := storeBuilderWithContext(ctx, t)
		defer cleanup()

		err := st.Put([]byte("foo"), []byte("FOO"))
		assert.NoError(t, err)

		cancel()
		err = st.Delete([]byte("foo"))
		assert.ErrorIs(t, err, context.Canceled)
	})
}

// TestStoreTruncate verifies Truncate behaviour.
func TestStoreTruncate(t *testing.T) {
	t.Run("Should succeed if store is empty", func(t *testing.T) {
		st, cleanup := storeBuilder(t)
		defer cleanup()

		err := st.Truncate()
		assert.NoError(t, err)
	})

	t.Run("Should truncate the store", func(t *testing.T) {
		st, cleanup := storeBuilder(t)
		defer cleanup()

		err := st.Put([]byte("foo"), []byte("FOO"))
		assert.NoError(t, err)
		err = st.Put([]byte("bar"), []byte("BAR"))
		assert.NoError(t, err)

		err = st.Truncate()
		assert.NoError(t, err)

		it := st.Iterator(nil)
		defer it.Close()
		it.First()
		assert.NoError(t, it.Error())
		require.False(t, it.Valid())
	})

	t.Run("Should fail if context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		st, cleanup := storeBuilderWithContext(ctx, t)
		defer cleanup()

		err := st.Put([]byte("foo"), []byte("FOO"))
		assert.NoError(t, err)

		cancel()
		err = st.Truncate()
		assert.ErrorIs(t, err, context.Canceled)
	})
}

// TestQueries test simple queries against the kv.
func TestQueries(t *testing.T) {
	t.Run("SELECT", func(t *testing.T) {
		ng := builder(t)
		defer func() {
			assert.NoError(t, ng.Close())
		}()

		db, err := genji.New(context.Background(), ng)
		assert.NoError(t, err)

		d, err := db.QueryDocument(`
			CREATE TABLE test;
			INSERT INTO test (a) VALUES (1), (2), (3), (4);
			SELECT COUNT(*) FROM test;
		`)
		assert.NoError(t, err)
		var count int
		err = document.Scan(d, &count)
		assert.NoError(t, err)
		require.Equal(t, 4, count)

		t.Run("ORDER BY", func(t *testing.T) {
			st, err := db.Query("SELECT * FROM test ORDER BY a DESC")
			assert.NoError(t, err)
			defer st.Close()

			var i int
			err = st.Iterate(func(d types.Document) error {
				var a int
				err := document.Scan(d, &a)
				assert.NoError(t, err)
				require.Equal(t, 4-i, a)
				i++
				return nil
			})
			assert.NoError(t, err)
		})
	})

	t.Run("INSERT", func(t *testing.T) {
		ng := builder(t)
		defer func() {
			assert.NoError(t, ng.Close())
		}()

		db, err := genji.New(context.Background(), ng)
		assert.NoError(t, err)

		err = db.Exec(`
			CREATE TABLE test;
			INSERT INTO test (a) VALUES (1), (2), (3), (4);
		`)
		assert.NoError(t, err)
	})

	t.Run("UPDATE", func(t *testing.T) {
		ng := builder(t)
		defer func() {
			assert.NoError(t, ng.Close())
		}()

		db, err := genji.New(context.Background(), ng)
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
		err = testutil.IteratorToJSONArray(&buf, st)
		assert.NoError(t, err)
		require.JSONEq(t, `[{"a": 5},{"a": 5},{"a": 5},{"a": 5}]`, buf.String())
	})

	t.Run("DELETE", func(t *testing.T) {
		ng := builder(t)
		defer func() {
			assert.NoError(t, ng.Close())
		}()

		db, err := genji.New(context.Background(), ng)
		assert.NoError(t, err)

		err = db.Exec("CREATE TABLE test")
		assert.NoError(t, err)

		err = db.Update(func(tx *genji.Tx) error {
			for i := 1; i < 200; i++ {
				err = tx.Exec("INSERT INTO test (a) VALUES (?)", i)
				assert.NoError(t, err)
			}
			return nil
		})
		assert.NoError(t, err)

		d, err := db.QueryDocument(`
			DELETE FROM test WHERE a > 2;
			SELECT COUNT(*) FROM test;
		`)
		assert.NoError(t, err)
		var count int
		err = document.Scan(d, &count)
		assert.NoError(t, err)
		require.Equal(t, 2, count)
	})
}

// TestQueriesSameTransaction test simple queries in the same transaction.
func TestQueriesSameTransaction(t *testing.T) {
	t.Run("SELECT", func(t *testing.T) {
		ng := builder(t)
		defer func() {
			assert.NoError(t, ng.Close())
		}()

		db, err := genji.New(context.Background(), ng)
		assert.NoError(t, err)

		err = db.Update(func(tx *genji.Tx) error {
			d, err := tx.QueryDocument(`
				CREATE TABLE test;
				INSERT INTO test (a) VALUES (1), (2), (3), (4);
				SELECT COUNT(*) FROM test;
			`)
			assert.NoError(t, err)
			var count int
			err = document.Scan(d, &count)
			assert.NoError(t, err)
			require.Equal(t, 4, count)
			return nil
		})
		assert.NoError(t, err)
	})

	t.Run("INSERT", func(t *testing.T) {
		ng := builder(t)
		defer func() {
			assert.NoError(t, ng.Close())
		}()

		db, err := genji.New(context.Background(), ng)
		assert.NoError(t, err)

		err = db.Update(func(tx *genji.Tx) error {
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
		ng := builder(t)
		defer func() {
			assert.NoError(t, ng.Close())
		}()

		db, err := genji.New(context.Background(), ng)
		assert.NoError(t, err)

		err = db.Update(func(tx *genji.Tx) error {
			st, err := tx.Query(`
				CREATE TABLE test;
				INSERT INTO test (a) VALUES (1), (2), (3), (4);
				UPDATE test SET a = 5;
				SELECT * FROM test;
			`)
			assert.NoError(t, err)
			defer st.Close()
			var buf bytes.Buffer
			err = testutil.IteratorToJSONArray(&buf, st)
			assert.NoError(t, err)
			require.JSONEq(t, `[{"a": 5},{"a": 5},{"a": 5},{"a": 5}]`, buf.String())
			return nil
		})
		assert.NoError(t, err)
	})

	t.Run("DELETE", func(t *testing.T) {
		ng := builder(t)
		defer func() {
			assert.NoError(t, ng.Close())
		}()

		db, err := genji.New(context.Background(), ng)
		assert.NoError(t, err)

		err = db.Update(func(tx *genji.Tx) error {
			d, err := tx.QueryDocument(`
			CREATE TABLE test;
			INSERT INTO test (a) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
			DELETE FROM test WHERE a > 2;
			SELECT COUNT(*) FROM test;
		`)
			assert.NoError(t, err)
			var count int
			document.Scan(d, &count)
			assert.NoError(t, err)
			require.Equal(t, 2, count)
			return nil
		})
		assert.NoError(t, err)
	})
}

func TestTransient(t *testing.T) {
	ng := testutil.NewEngine(t)

	ts, err := ng.NewTransientStore(context.Background())
	assert.NoError(t, err)

	dir := ts.Path

	err = ts.Put([]byte("foo"), []byte("bar"))
	assert.NoError(t, err)

	it := ts.Iterator(nil)
	defer it.Close()

	it.SeekGE([]byte("foo"))
	require.True(t, it.Valid())

	err = ts.Drop(context.Background())
	assert.NoError(t, err)

	_, err = os.Stat(dir)
	require.True(t, os.IsNotExist(err))
}

func tempDir(t testing.TB) string {
	dir, err := ioutil.TempDir("", "genji")
	assert.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return dir
}
