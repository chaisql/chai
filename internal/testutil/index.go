package testutil

import (
	"testing"

	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
)

func NewKey(t testing.TB, values ...types.Value) *tree.Key {
	t.Helper()

	return tree.NewKey(values...)
}
