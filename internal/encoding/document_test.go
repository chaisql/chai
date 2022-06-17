package encoding_test

import (
	"fmt"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/encoding"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func makeByteSlice(b ...byte) []byte {
	return b
}

func mergeByteSlices(b ...[]byte) []byte {
	var out []byte
	for _, b := range b {
		out = append(out, b...)
	}
	return out
}

func TestEncodeDecodeDocument(t *testing.T) {
	tests := []struct {
		input   types.Document
		want    [][]byte
		wantDoc types.Document
	}{
		{testutil.MakeDocument(t, `{}`), [][]byte{{byte(encoding.DocumentValue), makeUvarint(0)[0]}}, testutil.MakeDocument(t, `{}`)},
		{testutil.MakeDocument(t, `{"a": 1}`), [][]byte{
			makeByteSlice(byte(encoding.DocumentValue)),
			makeUvarint(1),
			encoding.EncodeText(nil, "a"),
			encoding.EncodeInt(nil, 1),
		}, testutil.MakeDocument(t, `{"a": 1.0}`)},
		{testutil.MakeDocument(t, `{"a": {"b": 1}, "c": 1}`), [][]byte{
			makeByteSlice(byte(encoding.DocumentValue)),
			makeUvarint(2),
			encoding.EncodeText(nil, "a"), makeByteSlice(byte(encoding.DocumentValue)), makeUvarint(1), encoding.EncodeText(nil, "b"), encoding.EncodeInt(nil, 1),
			encoding.EncodeText(nil, "c"), encoding.EncodeInt(nil, 1),
		},
			testutil.MakeDocument(t, `{"a": {"b": 1.0}, "c": 1.0}`),
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s", test.input), func(t *testing.T) {
			got, err := encoding.EncodeDocument(nil, test.input)
			require.NoError(t, err)

			require.Equal(t, mergeByteSlices(test.want...), got)

			x := encoding.DecodeDocument(got, true)
			testutil.RequireDocEqual(t, test.wantDoc, x)
		})
	}
}

func TestDocumentGetByField(t *testing.T) {
	tests := []struct {
		input   types.Document
		path    document.Path
		want    types.Value
		wantErr error
	}{
		{testutil.MakeDocument(t, `{}`), document.NewPath("a"), nil, types.ErrFieldNotFound},
		{testutil.MakeDocument(t, `{"a": 1}`), document.NewPath("a"), types.NewDoubleValue(1), nil},
		{testutil.MakeDocument(t, `{"a": 1}`), document.NewPath("b"), nil, types.ErrFieldNotFound},
		{testutil.MakeDocument(t, `{"a": {"b": 1}}`), document.NewPath("a", "b"), types.NewDoubleValue(1), nil},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s", test.input), func(t *testing.T) {
			got, err := encoding.EncodeDocument(nil, test.input)
			require.NoError(t, err)

			x := encoding.DecodeDocument(got, true)
			v, err := test.path.GetValueFromDocument(x)
			if test.wantErr != nil {
				require.Equal(t, test.wantErr, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.want, v)
			}
		})
	}
}
