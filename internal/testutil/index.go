package testutil

import (
	"testing"

	"github.com/chaisql/chai/internal/tree"
	"github.com/chaisql/chai/internal/types"
)

func NewKey(t testing.TB, values ...types.Value) *tree.Key {
	t.Helper()

	return tree.NewKey(values...)
}
