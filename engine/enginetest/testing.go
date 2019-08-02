// Package enginetest defines a list of tests that can be used to test
// a complete or partial engine implementation.
package enginetest

import (
	"testing"

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
		// {"Transaction/ListStores", TestTransactionListStores},
		{"Store/AscendGreaterOrEqual", TestStoreAscendGreaterOrEqual},
		{"Store/DescendLessOrEqual", TestStoreDescendLessOrEqual},
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
		st, err := tx.Store("store1")
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
				func(tx engine.Transaction, err *error) { _, *err = tx.Store("store") },
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
					st, er := tx.Store("store")
					require.NoError(t, er)
					require.NoError(t, st.Put([]byte("foo"), []byte("FOO")))
				},
				func(tx engine.Transaction, err *error) {
					st, er := tx.Store("store")
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
				func(tx engine.Transaction, err *error) { _, *err = tx.Store("store") },
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

		st, err := tx.Store("store")
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

		_, err = tx.Store("store")
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
		sta, err := tx.Store("storea")
		require.NoError(t, err)

		// fetch second store
		stb, err := tx.Store("storeb")
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

		_, err = tx.Store("store")
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
