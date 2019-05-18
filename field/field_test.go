package field_test

import (
	"testing"

	"github.com/asdine/genji/field"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecode(t *testing.T) {
	tests := []struct {
		name     string
		expected interface{}
		enc      func() []byte
		dec      func([]byte) (interface{}, error)
	}{
		{"bool", true, func() []byte { return field.EncodeBool(true) }, func(v []byte) (interface{}, error) { return field.DecodeBool(v) }},
		{"uint", uint(10), func() []byte { return field.EncodeUint(10) }, func(v []byte) (interface{}, error) { return field.DecodeUint(v) }},
		{"uint8", uint8(10), func() []byte { return field.EncodeUint8(10) }, func(v []byte) (interface{}, error) { return field.DecodeUint8(v) }},
		{"uint16", uint16(10), func() []byte { return field.EncodeUint16(10) }, func(v []byte) (interface{}, error) { return field.DecodeUint16(v) }},
		{"uint32", uint32(10), func() []byte { return field.EncodeUint32(10) }, func(v []byte) (interface{}, error) { return field.DecodeUint32(v) }},
		{"uint64", uint64(10), func() []byte { return field.EncodeUint64(10) }, func(v []byte) (interface{}, error) { return field.DecodeUint64(v) }},
		{"int", int(-10), func() []byte { return field.EncodeInt(-10) }, func(v []byte) (interface{}, error) { return field.DecodeInt(v) }},
		{"int8", int8(-10), func() []byte { return field.EncodeInt8(-10) }, func(v []byte) (interface{}, error) { return field.DecodeInt8(v) }},
		{"int16", int16(-10), func() []byte { return field.EncodeInt16(-10) }, func(v []byte) (interface{}, error) { return field.DecodeInt16(v) }},
		{"int32", int32(-10), func() []byte { return field.EncodeInt32(-10) }, func(v []byte) (interface{}, error) { return field.DecodeInt32(v) }},
		{"int64", int64(-10), func() []byte { return field.EncodeInt64(-10) }, func(v []byte) (interface{}, error) { return field.DecodeInt64(v) }},
		{"float32", float32(-3.14), func() []byte { return field.EncodeFloat32(-3.14) }, func(v []byte) (interface{}, error) { return field.DecodeFloat32(v) }},
		{"float64", float64(-3.14), func() []byte { return field.EncodeFloat64(-3.14) }, func(v []byte) (interface{}, error) { return field.DecodeFloat64(v) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v := test.enc()
			actual, err := test.dec(v)
			require.NoError(t, err)
			require.Equal(t, test.expected, actual)
		})
	}
}
