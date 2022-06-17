package encoding_test

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/genjidb/genji/internal/encoding"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func makeUvarint(n int) []byte {
	var buf [10]byte
	i := binary.PutUvarint(buf[:], uint64(n))
	return buf[:i]
}

func TestEncodeDecodeArray(t *testing.T) {
	tests := []struct {
		input     types.Array
		want      []byte
		wantArray types.Array
	}{
		{testutil.MakeArray(t, `[]`), []byte{byte(encoding.ArrayValue), makeUvarint(0)[0]}, testutil.MakeArray(t, `[]`)},
		{testutil.MakeArray(t, `[1]`), []byte{byte(encoding.ArrayValue), makeUvarint(1)[0], encoding.EncodeInt(nil, 1)[0]}, testutil.MakeArray(t, `[1.0]`)},
		{testutil.MakeArray(t, `[1, []]`),
			[]byte{
				byte(encoding.ArrayValue),
				makeUvarint(2)[0],
				encoding.EncodeInt(nil, 1)[0],
				byte(encoding.ArrayValue), makeUvarint(0)[0],
			},
			testutil.MakeArray(t, `[1.0, []]`),
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%d", test.input), func(t *testing.T) {
			got, err := encoding.EncodeArray(nil, test.input)
			require.NoError(t, err)
			require.Equal(t, test.want, got)

			x := encoding.DecodeArray(got, true)
			testutil.RequireArrayEqual(t, test.wantArray, x)
		})
	}
}
