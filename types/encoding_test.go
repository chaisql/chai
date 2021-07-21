package types_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/binarysort"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestValueEncoder(t *testing.T) {
	tests := []struct {
		name string
		v    types.Value
	}{
		{"null", types.NewNullValue()},
		{"bool", types.NewBoolValue(true)},
		{"integer", types.NewIntegerValue(-10)},
		{"double", types.NewDoubleValue(-3.14)},
		{"text", types.NewTextValue("foo")},
		{"blob", types.NewBlobValue([]byte("bar"))},
		{"array", types.NewArrayValue(document.NewValueBuffer(
			types.NewBoolValue(true),
			types.NewIntegerValue(55),
			types.NewDoubleValue(789.58),
			types.NewArrayValue(document.NewValueBuffer(
				types.NewBoolValue(false),
				types.NewIntegerValue(100),
				types.NewTextValue("baz"),
			)),
			types.NewBlobValue([]byte("loo")),
			types.NewDocumentValue(
				document.NewFieldBuffer().
					Add("foo1", types.NewBoolValue(true)).
					Add("foo2", types.NewIntegerValue(55)).
					Add("foo3", types.NewArrayValue(document.NewValueBuffer(
						types.NewBoolValue(false),
						types.NewIntegerValue(100),
						types.NewTextValue("baz"),
					))),
			),
		))},
		{"document", types.NewDocumentValue(
			document.NewFieldBuffer().
				Add("foo1", types.NewBoolValue(true)).
				Add("foo2", types.NewIntegerValue(55)).
				Add("foo3", types.NewArrayValue(document.NewValueBuffer(
					types.NewBoolValue(false),
					types.NewIntegerValue(100),
					types.NewTextValue("baz"),
				))).
				Add("foo4", types.NewDocumentValue(
					document.NewFieldBuffer().
						Add("foo1", types.NewBoolValue(true)).
						Add("foo2", types.NewIntegerValue(55)).
						Add("foo3", types.NewArrayValue(document.NewValueBuffer(
							types.NewBoolValue(false),
							types.NewIntegerValue(100),
							types.NewTextValue("baz"),
						))),
				)),
		)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer

			enc := types.NewValueEncoder(&buf)
			err := enc.Encode(test.v)
			require.NoError(t, err)

			got, err := decodeValue(buf.Bytes())
			require.NoError(t, err)
			require.Equal(t, test.v, got)
		})
	}
}

// decodeValue decodes a value encoded with ValueEncoder.
func decodeValue(data []byte) (types.Value, error) {
	t := types.ValueType(data[0])
	data = data[1:]

	switch t {
	case types.NullValue:
		return types.NewNullValue(), nil
	case types.BlobValue:
		t, err := binarysort.DecodeBase64(data)
		if err != nil {
			return nil, err
		}
		return types.NewBlobValue(t), nil
	case types.TextValue:
		t, err := binarysort.DecodeBase64(data)
		if err != nil {
			return nil, err
		}
		return types.NewTextValue(string(t)), nil
	case types.BoolValue:
		b, err := binarysort.DecodeBool(data)
		if err != nil {
			return nil, err
		}
		return types.NewBoolValue(b), nil
	case types.IntegerValue:
		x, err := binarysort.DecodeInt64(data)
		if err != nil {
			return nil, err
		}
		return types.NewIntegerValue(x), nil
	case types.DoubleValue:
		x, err := binarysort.DecodeFloat64(data)
		if err != nil {
			return nil, err
		}
		return types.NewDoubleValue(x), nil
	case types.ArrayValue:
		a, _, err := decodeArray(data)
		if err != nil {
			return nil, err
		}
		return types.NewArrayValue(a), nil
	case types.DocumentValue:
		d, _, err := decodeDocument(data)
		if err != nil {
			return nil, err
		}
		return types.NewDocumentValue(d), nil
	}

	return nil, errors.New("unknown type")
}

func decodeValueUntil(data []byte, delim, end byte) (types.Value, int, error) {
	t := types.ValueType(data[0])
	i := 1

	switch t {
	case types.ArrayValue:
		a, n, err := decodeArray(data[i:])
		i += n
		if err != nil {
			return nil, i, err
		}
		return types.NewArrayValue(a), i, nil
	case types.DocumentValue:
		d, n, err := decodeDocument(data[i:])
		i += n
		if err != nil {
			return nil, i, err
		}
		return types.NewDocumentValue(d), i, nil
	case types.NullValue:
	case types.BoolValue:
		i++
	case types.IntegerValue, types.DoubleValue:
		if i+8 < len(data) && (data[i+8] == delim || data[i+8] == end) {
			i += 8
		} else {
			return nil, 0, errors.New("malformed " + t.String())
		}
	case types.BlobValue, types.TextValue:
		for i < len(data) && data[i] != delim && data[i] != end {
			i++
		}
	default:
		return nil, 0, errors.New("invalid type character")
	}

	v, err := decodeValue(data[:i])
	return v, i, err
}

func decodeArray(data []byte) (types.Array, int, error) {
	var vb document.ValueBuffer

	var readCount int
	for len(data) > 0 && data[0] != types.ArrayEnd {
		v, i, err := decodeValueUntil(data, types.ArrayValueDelim, types.ArrayEnd)
		if err != nil {
			return nil, i, err
		}

		vb.Append(v)

		// skip the delimiter
		if data[i] == types.ArrayValueDelim {
			i++
		}

		readCount += i

		data = data[i:]
	}

	// skip the array end character
	readCount++

	return &vb, readCount, nil
}

func decodeDocument(data []byte) (types.Document, int, error) {
	var fb document.FieldBuffer

	var readCount int
	for len(data) > 0 && data[0] != types.DocumentEnd {
		i := 0

		for i < len(data) && data[i] != types.DocumentValueDelim {
			i++
		}

		field, err := binarysort.DecodeBase64(data[:i])
		if err != nil {
			return nil, 0, err
		}

		// skip the delimiter
		i++

		if i >= len(data) {
			return nil, 0, errors.New("invalid end of input")
		}

		readCount += i

		data = data[i:]

		v, i, err := decodeValueUntil(data, types.DocumentValueDelim, types.DocumentEnd)
		if err != nil {
			return nil, i, err
		}

		fb.Add(string(field), v)

		// skip the delimiter
		if data[i] == types.DocumentValueDelim {
			i++
		}

		readCount += i

		data = data[i:]
	}

	// skip the document end character
	readCount++

	return &fb, readCount, nil
}
