package encoding_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/chaisql/chai/internal/encoding"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeText(t *testing.T) {
	a100 := append([]byte{encoding.TextValue}, makeUvarint(100)...)
	a100 = append(a100, bytes.Repeat([]byte{'a'}, 100)...)
	tests := []struct {
		input string
		want  []byte
	}{
		{"", []byte{encoding.TextValue, makeUvarint(0)[0]}},
		{"a", []byte{encoding.TextValue, makeUvarint(1)[0], 'a'}},
		{strings.Repeat("a", 100), a100},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.input), func(t *testing.T) {
			got := encoding.EncodeText(nil, test.input)
			require.Equal(t, test.want, got)

			x, n := encoding.DecodeText(got)
			require.Equal(t, test.input, x)
			require.Equal(t, len(test.input)+len(makeUvarint(len(test.input)))+1, n)
		})
	}
}

func TestEncodeDecodeBlob(t *testing.T) {
	a100 := append([]byte{encoding.BlobValue}, makeUvarint(100)...)
	a100 = append(a100, bytes.Repeat([]byte{'a'}, 100)...)
	tests := []struct {
		input []byte
		want  []byte
	}{
		{[]byte{}, []byte{encoding.BlobValue, makeUvarint(0)[0]}},
		{[]byte{'a'}, []byte{encoding.BlobValue, makeUvarint(1)[0], 'a'}},
		{bytes.Repeat([]byte{'a'}, 100), a100},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.input), func(t *testing.T) {
			got := encoding.EncodeBlob(nil, test.input)
			require.Equal(t, test.want, got)

			x, n := encoding.DecodeBlob(got)
			require.Equal(t, test.input, x)
			require.Equal(t, len(test.input)+len(makeUvarint(len(test.input)))+1, n)
		})
	}
}
