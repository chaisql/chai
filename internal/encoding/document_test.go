package encoding_test

import (
	"fmt"
	"testing"

	"github.com/genjidb/genji/internal/encoding"
	"github.com/genjidb/genji/internal/object"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/types"
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

func TestEncodeDecodeObject(t *testing.T) {
	tests := []struct {
		input   types.Object
		want    [][]byte
		wantDoc types.Object
	}{
		{testutil.MakeObject(t, `{}`), [][]byte{{byte(encoding.ObjectValue), makeUvarint(0)[0]}}, testutil.MakeObject(t, `{}`)},
		{testutil.MakeObject(t, `{"a": 1}`), [][]byte{
			makeByteSlice(byte(encoding.ObjectValue)),
			makeUvarint(1),
			encoding.EncodeText(nil, "a"),
			encoding.EncodeInt(nil, 1),
		}, testutil.MakeObject(t, `{"a": 1.0}`)},
		{testutil.MakeObject(t, `{"a": {"b": 1}, "c": 1}`), [][]byte{
			makeByteSlice(byte(encoding.ObjectValue)),
			makeUvarint(2),
			encoding.EncodeText(nil, "a"), makeByteSlice(byte(encoding.ObjectValue)), makeUvarint(1), encoding.EncodeText(nil, "b"), encoding.EncodeInt(nil, 1),
			encoding.EncodeText(nil, "c"), encoding.EncodeInt(nil, 1),
		},
			testutil.MakeObject(t, `{"a": {"b": 1.0}, "c": 1.0}`),
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s", test.input), func(t *testing.T) {
			got, err := encoding.EncodeObject(nil, test.input)
			require.NoError(t, err)

			require.Equal(t, mergeByteSlices(test.want...), got)

			x := encoding.DecodeObject(got, true)
			testutil.RequireObjEqual(t, test.wantDoc, x)
		})
	}
}

func TestObjectGetByField(t *testing.T) {
	tests := []struct {
		input   types.Object
		path    object.Path
		want    types.Value
		wantErr error
	}{
		{testutil.MakeObject(t, `{}`), object.NewPath("a"), nil, types.ErrFieldNotFound},
		{testutil.MakeObject(t, `{"a": 1}`), object.NewPath("a"), types.NewDoubleValue(1), nil},
		{testutil.MakeObject(t, `{"a": 1}`), object.NewPath("b"), nil, types.ErrFieldNotFound},
		{testutil.MakeObject(t, `{"a": {"b": 1}}`), object.NewPath("a", "b"), types.NewDoubleValue(1), nil},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s", test.input), func(t *testing.T) {
			got, err := encoding.EncodeObject(nil, test.input)
			require.NoError(t, err)

			x := encoding.DecodeObject(got, true)
			v, err := test.path.GetValueFromObject(x)
			if test.wantErr != nil {
				require.Equal(t, test.wantErr, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.want, v)
			}
		})
	}
}
