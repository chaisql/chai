package testutil

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func DumpPebble(t testing.TB, pdb *pebble.DB) {
	t.Helper()
	it, err := pdb.NewIter(nil)
	require.NoError(t, err)

	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("%v: %v\n", it.Key(), it.Value())
	}

	err = it.Close()
	require.NoError(t, err)
}
