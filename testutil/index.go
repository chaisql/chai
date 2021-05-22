package testutil

import (
	"testing"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/stretchr/testify/require"
)

// GetIndexContent iterates over the entire index and returns all the key-value pairs in order.
func GetIndexContent(t testing.TB, tx *database.Transaction, indexName string) []KV {
	t.Helper()

	idx, err := tx.Catalog.GetIndex(tx, indexName)
	require.NoError(t, err)

	var content []KV
	err = idx.AscendGreaterOrEqual([]document.Value{{}}, func(val, key []byte) error {
		content = append(content, KV{
			Key:   append([]byte{}, val...),
			Value: append([]byte{}, key...),
		})
		return nil
	})
	require.NoError(t, err)

	return content
}
