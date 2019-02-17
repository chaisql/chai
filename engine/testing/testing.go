// Package testing defines a list of tests that can be used to test
// a complete or partial engine implementation.
package testing

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
	t.Run("Transaction/Commit-Rollback", func(t *testing.T) {
		TestTransactionCommitRollback(t, builder)
	})

	t.Run("Engine", func(t *testing.T) {
		TestEngine(t, builder)
	})
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
		tx, err := ng.Begin(false)
		require.NoError(t, err)
		defer tx.Rollback()

		tests := []struct {
			name string
			err  error
			fn   func(*error)
		}{
			{"CreateTable", engine.ErrTransactionReadOnly, func(err *error) { _, *err = tx.CreateTable("test") }},
			{"CreateIndex", engine.ErrTransactionReadOnly, func(err *error) { _, *err = tx.CreateIndex("test", "idx") }},
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
			writeFn func(engine.Transaction, *error)
			readFn  func(engine.Transaction, *error)
		}{
			{
				"CreateTable",
				func(tx engine.Transaction, err *error) { _, *err = tx.CreateTable("test") },
				func(tx engine.Transaction, err *error) { _, *err = tx.Table("test") },
			},
			{
				"CreateIndex",
				func(tx engine.Transaction, err *error) {
					_, er := tx.CreateTable("test")
					if er != nil {
						*err = er
						return
					}

					_, *err = tx.CreateIndex("test", "idx")
				},
				func(tx engine.Transaction, err *error) { _, *err = tx.Index("test", "idx") },
			},
		}

		for _, test := range tests {
			t.Run(test.name+"/rollback", func(t *testing.T) {
				ng, cleanup := builder()
				defer cleanup()

				tx, err := ng.Begin(true)
				require.NoError(t, err)
				defer tx.Rollback()

				test.writeFn(tx, &err)
				require.NoError(t, err)

				err = tx.Rollback()
				require.NoError(t, err)

				tx, err = ng.Begin(false)
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
				tx, err := ng.Begin(true)
				require.NoError(t, err)
				defer tx.Rollback()

				test.writeFn(tx, &err)
				require.NoError(t, err)

				err = tx.Commit()
				require.NoError(t, err)

				tx, err = ng.Begin(false)
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
				"CreateTable",
				func(tx engine.Transaction, err *error) { _, *err = tx.CreateTable("test3") },
				func(tx engine.Transaction, err *error) { _, *err = tx.Table("test3") },
			},
			{
				"CreateIndex",
				func(tx engine.Transaction, err *error) {
					_, er := tx.CreateTable("test4")
					if er != nil {
						*err = er
						return
					}

					_, *err = tx.CreateIndex("test4", "idx")
				},
				func(tx engine.Transaction, err *error) { _, *err = tx.Index("test4", "idx") },
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
