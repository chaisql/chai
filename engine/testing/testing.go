// Package testing defines a list of tests that can be used to test
// a complete or partial engine implementation.
package testing

import (
	"testing"

	"github.com/asdine/genji/engine"
	"github.com/stretchr/testify/require"
)

// TestEngine runs a list of tests against the provided engine.
// It tests the entire engine, including transactions.
func TestEngine(t *testing.T, ng engine.Engine) {
	t.Run("Close", func(t *testing.T) {
		require.NoError(t, ng.Close())
	})

	t.Run("Transaction", func(t *testing.T) {
		TestTransaction(t, ng)
	})
}

// TestTransaction runs a list of tests against transactions created
// thanks to the provided engine.
// It tests the entire transaction, including table and index implementations.
// It is called by TestEngine.
func TestTransaction(t *testing.T, ng engine.Engine) {
	t.Run("Commit after rollback should fail", func(t *testing.T) {
		tx, err := ng.Begin(false)
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)

		err = tx.Commit()
		require.Error(t, err)
	})

	t.Run("Rollback after commit should not fail", func(t *testing.T) {
		tx, err := ng.Begin(false)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)
	})

	t.Run("Commit after commit should fail", func(t *testing.T) {
		tx, err := ng.Begin(false)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		err = tx.Commit()
		require.Error(t, err)
	})

	t.Run("Rollback after rollback should not fail", func(t *testing.T) {
		tx, err := ng.Begin(false)
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)
	})

	t.Run("Read-Only", func(t *testing.T) {
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
		require.NoError(t, ng.Close())
	})
}
