// Package encodingtest provides a test suite for testing codec implementations.
package encodingtest

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
	"github.com/stretchr/testify/require"
)

// TestCodec runs a list of tests on the given codec.
func TestCodec(t *testing.T, codecBuilder func() encoding.Codec) {
	t.Run("Encoding using a nil reader should fail", func(t *testing.T) {
		codec := codecBuilder()
		err := codec.NewEncoder(nil).EncodeDocument(document.NewFieldBuffer().Add("a", document.NewBoolValue(true)))
		require.Error(t, err)
	})
}
