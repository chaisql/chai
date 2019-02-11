package memory

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransaction(t *testing.T) {
	t.Run("index", func(t *testing.T) {
		ng := NewEngine()
		tx, err := ng.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()

		_, err = tx.Index("table", "test")
		require.Error(t, err)
		idx1, err := tx.CreateIndex("table", "test")
		require.NoError(t, err)
		idx2, err := tx.Index("table", "test")
		require.NoError(t, err)
		require.Equal(t, idx1, idx2)
	})
}
