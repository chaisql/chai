package testutil

import (
	"testing"

	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
)

// GetIndexContent iterates over the entire index and returns all the key-value pairs in order.
func GetIndexContent(t testing.TB, tx *database.Transaction, catalog database.Catalog, indexName string) []KV {
	t.Helper()

	idx, err := catalog.GetIndex(tx, indexName)
	assert.NoError(t, err)

	var content []KV
	err = idx.AscendGreaterOrEqual([]types.Value{nil}, func(val, key []byte) error {
		content = append(content, KV{
			Key:   append([]byte{}, val...),
			Value: append([]byte{}, key...),
		})
		return nil
	})
	assert.NoError(t, err)

	return content
}
