package encoding

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/asdine/genji/document"
	"github.com/asdine/genji/pkg/bytesutil"
	"github.com/stretchr/testify/require"
)

func TestDecodeValueFromDocument(t *testing.T) {
	doc := document.NewFieldBuffer().
		Add("age", document.NewInt64Value(10)).
		Add("address", document.NewNullValue()).
		Add("name", document.NewTextValue("john")).
		Add("d", document.NewDurationValue(10*time.Nanosecond))

	data, err := EncodeDocument(doc)
	require.NoError(t, err)

	v, err := decodeValueFromDocument(data, "age")
	require.NoError(t, err)
	require.Equal(t, document.NewInt64Value(10), v)

	v, err = decodeValueFromDocument(data, "address")
	require.NoError(t, err)
	require.Equal(t, document.NewNullValue(), v)

	v, err = decodeValueFromDocument(data, "name")
	require.NoError(t, err)
	require.Equal(t, document.NewTextValue("john"), v)

	v, err = decodeValueFromDocument(data, "d")
	require.NoError(t, err)
	require.Equal(t, document.NewDurationValue(10*time.Nanosecond), v)
}

func TestEncodeDecode(t *testing.T) {
	userMapDoc, err := document.NewFromMap(map[string]interface{}{
		"age":  10,
		"name": "john",
	})
	require.NoError(t, err)

	addressMapDoc, err := document.NewFromMap(map[string]string{
		"city":    "Ajaccio",
		"country": "France",
	})
	require.NoError(t, err)

	tests := []struct {
		name     string
		d        document.Document
		expected string
	}{
		{
			"document.FieldBuffer",
			document.NewFieldBuffer().
				Add("age", document.NewInt64Value(10)).
				Add("name", document.NewTextValue("john")),
			`{"age": 10, "name": "john"}`,
		},
		{
			"Map",
			userMapDoc,
			`{"age": 10, "name": "john"}`,
		},
		{
			"Nested Document",
			document.NewFieldBuffer().
				Add("age", document.NewInt64Value(10)).
				Add("name", document.NewTextValue("john")).
				Add("address", document.NewDocumentValue(addressMapDoc)),
			`{"age": 10, "name": "john", "address": {"city": "Ajaccio", "country": "France"}}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			enc, err := EncodeDocument(test.d)
			require.NoError(t, err)
			var buf bytes.Buffer
			err = document.ToJSON(&buf, DecodeDocument(enc))
			require.NoError(t, err)
			require.JSONEq(t, test.expected, buf.String())
		})
	}
}

func TestDecodeDocument(t *testing.T) {
	mapDoc, err := document.NewFromMap(map[string]string{
		"city":    "Ajaccio",
		"country": "France",
	})
	require.NoError(t, err)

	doc := document.NewFieldBuffer().
		Add("age", document.NewInt64Value(10)).
		Add("name", document.NewTextValue("john")).
		Add("address", document.NewDocumentValue(mapDoc))

	data, err := EncodeDocument(doc)
	require.NoError(t, err)

	ec := DecodeDocument(data)
	v, err := ec.GetByField("age")
	require.NoError(t, err)
	require.Equal(t, document.NewInt64Value(10), v)
	v, err = ec.GetByField("address")
	require.NoError(t, err)
	var expected, actual bytes.Buffer
	err = document.ToJSON(&expected, document.NewFieldBuffer().Add("address", document.NewDocumentValue(mapDoc)))
	require.NoError(t, err)
	err = document.ToJSON(&actual, document.NewFieldBuffer().Add("address", v))
	require.NoError(t, err)
	require.JSONEq(t, expected.String(), actual.String())

	var i int
	err = ec.Iterate(func(f string, v document.Value) error {
		switch f {
		case "age":
			require.Equal(t, document.NewInt64Value(10), v)
		case "address":
			var expected, actual bytes.Buffer
			err = document.ToJSON(&expected, document.NewFieldBuffer().Add("address", document.NewDocumentValue(mapDoc)))
			require.NoError(t, err)
			err = document.ToJSON(&actual, document.NewFieldBuffer().Add(f, v))
			require.NoError(t, err)
			require.JSONEq(t, expected.String(), actual.String())
		case "name":
			require.Equal(t, document.NewTextValue("john"), v)
		}
		i++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, i)
}

func TestEncodeArray(t *testing.T) {
	mapDoc, err := document.NewFromMap(map[string]string{
		"city":    "Ajaccio",
		"country": "France",
	})
	require.NoError(t, err)

	tests := []struct {
		name     string
		a        document.Array
		expected string
	}{
		{
			"Complex array",
			document.NewValueBuffer().
				Append(document.NewInt64Value(10)).
				Append(document.NewTextValue("john")).
				Append(document.NewDocumentValue(mapDoc)).
				Append(document.NewArrayValue(document.NewValueBuffer().Append(document.NewInt64Value(11)))),
			`[10, "john", {"city": "Ajaccio", "country": "France"}, [11]]`,
		},
		{
			"Empty array",
			document.NewValueBuffer(),
			`[]`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := EncodeArray(test.a)
			require.NoError(t, err)
			var buf bytes.Buffer
			document.ArrayToJSON(&buf, DecodeArray(data))
			require.JSONEq(t, test.expected, buf.String())
		})
	}
}

func BenchmarkEncodeDocument(b *testing.B) {
	var buf document.FieldBuffer

	for i := int64(0); i < 100; i++ {
		buf.Add(fmt.Sprintf("name-%d", i), document.NewInt64Value(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeDocument(&buf)
	}
}

func BenchmarkFormatDecode(b *testing.B) {
	var buf document.FieldBuffer

	for i := int64(0); i < 100; i++ {
		buf.Add(fmt.Sprintf("name-%d", i), document.NewInt64Value(i))
	}

	data, err := EncodeDocument(&buf)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var f Format
		f.Decode(data)
	}
}

func BenchmarkGetByField(b *testing.B) {
	var buf document.FieldBuffer

	for i := int64(0); i < 100; i++ {
		buf.Add(fmt.Sprintf("name-%d", i), document.NewInt64Value(i))
	}

	data, err := EncodeDocument(&buf)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeDocument(data).GetByField("name-99")
	}
}

func BenchmarkDecodeDocument(b *testing.B) {
	var buf document.FieldBuffer

	for i := int64(0); i < 100; i++ {
		buf.Add(fmt.Sprintf("name-%d", i), document.NewInt64Value(i))
	}

	data, err := EncodeDocument(&buf)
	require.NoError(b, err)

	ec := DecodeDocument(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ec.Iterate(func(string, document.Value) error {
			return nil
		})
	}
}

func TestValueEncodeDecode(t *testing.T) {
	tests := []struct {
		name     string
		expected interface{}
		enc      func() []byte
		dec      func([]byte) (interface{}, error)
	}{
		{"bytes", []byte("foo"), func() []byte { return EncodeBlob([]byte("foo")) }, func(buf []byte) (interface{}, error) { return DecodeBlob(buf) }},
		{"string", "bar", func() []byte { return EncodeText("bar") }, func(buf []byte) (interface{}, error) { return DecodeText(buf) }},
		{"bool", true, func() []byte { return EncodeBool(true) }, func(buf []byte) (interface{}, error) { return DecodeBool(buf) }},
		{"uint", uint(10), func() []byte { return EncodeUint(10) }, func(buf []byte) (interface{}, error) { return DecodeUint(buf) }},
		{"uint8", uint8(10), func() []byte { return EncodeUint8(10) }, func(buf []byte) (interface{}, error) { return DecodeUint8(buf) }},
		{"uint16", uint16(10), func() []byte { return EncodeUint16(10) }, func(buf []byte) (interface{}, error) { return DecodeUint16(buf) }},
		{"uint32", uint32(10), func() []byte { return EncodeUint32(10) }, func(buf []byte) (interface{}, error) { return DecodeUint32(buf) }},
		{"uint64", uint64(10), func() []byte { return EncodeUint64(10) }, func(buf []byte) (interface{}, error) { return DecodeUint64(buf) }},
		{"int", int(-10), func() []byte { return EncodeInt(-10) }, func(buf []byte) (interface{}, error) { return DecodeInt(buf) }},
		{"int8", int8(-10), func() []byte { return EncodeInt8(-10) }, func(buf []byte) (interface{}, error) { return DecodeInt8(buf) }},
		{"int16", int16(-10), func() []byte { return EncodeInt16(-10) }, func(buf []byte) (interface{}, error) { return DecodeInt16(buf) }},
		{"int32", int32(-10), func() []byte { return EncodeInt32(-10) }, func(buf []byte) (interface{}, error) { return DecodeInt32(buf) }},
		{"int64", int64(-10), func() []byte { return EncodeInt64(-10) }, func(buf []byte) (interface{}, error) { return DecodeInt64(buf) }},
		{"float64", float64(-3.14), func() []byte { return EncodeFloat64(-3.14) }, func(buf []byte) (interface{}, error) { return DecodeFloat64(buf) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buf := test.enc()
			actual, err := test.dec(buf)
			require.NoError(t, err)
			require.Equal(t, test.expected, actual)
		})
	}
}

const Rng = 1000

func TestOrdering(t *testing.T) {
	tests := []struct {
		name     string
		min, max int
		enc      func(int) []byte
	}{
		{"uint", 0, 1000, func(i int) []byte { return EncodeUint(uint(i)) }},
		{"uint8", 0, 255, func(i int) []byte { return EncodeUint8(uint8(i)) }},
		{"uint16", 0, 1000, func(i int) []byte { return EncodeUint16(uint16(i)) }},
		{"uint32", 0, 1000, func(i int) []byte { return EncodeUint32(uint32(i)) }},
		{"uint64", 0, 1000, func(i int) []byte { return EncodeUint64(uint64(i)) }},
		{"int", -1000, 1000, func(i int) []byte { return EncodeInt(i) }},
		{"int8", -100, 100, func(i int) []byte { return EncodeInt8(int8(i)) }},
		{"int16", -1000, 1000, func(i int) []byte { return EncodeInt16(int16(i)) }},
		{"int32", -1000, 1000, func(i int) []byte { return EncodeInt32(int32(i)) }},
		{"int64", -1000, 1000, func(i int) []byte { return EncodeInt64(int64(i)) }},
		{"float64", -1000, 1000, func(i int) []byte { return EncodeFloat64(float64(i)) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var prev []byte
			for i := test.min; i < test.max; i++ {
				cur := test.enc(i)
				if prev == nil {
					prev = cur
					continue
				}

				require.Equal(t, -1, bytesutil.CompareBytes(prev, cur))
				prev = cur
			}
		})
	}
}
