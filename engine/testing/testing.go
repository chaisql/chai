// Package testing defines a list of tests that can be used to test
// a complete or partial engine implementation.
package testing

import (
	"testing"

	"github.com/asdine/genji/engine"
	"github.com/stretchr/testify/require"
)

// TestSuite tests an entire engine, transaction and related types
// needed to implement a Genji engine.
func TestSuite(t *testing.T, ng engine.Engine) {
	t.Run("Transaction", func(t *testing.T) {
		TestTransaction(t, ng)
	})

	t.Run("Engine", func(t *testing.T) {
		TestEngine(t, ng)
	})
}

// TestEngine runs a list of tests against the provided engine.
func TestEngine(t *testing.T, ng engine.Engine) {
	t.Run("Close", func(t *testing.T) {
		require.NoError(t, ng.Close())
	})
}

// TestTransaction runs a list of tests against transactions created
// thanks to the provided engine.
func TestTransaction(t *testing.T, ng engine.Engine) {
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
				func(tx engine.Transaction, err *error) { _, *err = tx.CreateTable("test1") },
				func(tx engine.Transaction, err *error) { _, *err = tx.Table("test1") },
			},
			{
				"CreateIndex",
				func(tx engine.Transaction, err *error) {
					_, er := tx.CreateTable("test2")
					if er != nil {
						*err = er
						return
					}

					_, *err = tx.CreateIndex("test2", "idx")
				},
				func(tx engine.Transaction, err *error) { _, *err = tx.Index("test2", "idx") },
			},
		}

		for _, test := range tests {
			t.Run(test.name+"/rollback", func(t *testing.T) {
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
