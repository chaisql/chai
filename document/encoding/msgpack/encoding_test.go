package msgpack

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/genjidb/genji/document"
	"github.com/stretchr/testify/require"
)

func TestGetByField(t *testing.T) {
	doc := document.NewFieldBuffer().
		Add("age", document.NewInt64Value(10)).
		Add("address", document.NewNullValue()).
		Add("name", document.NewTextValue("john")).
		Add("d", document.NewDurationValue(10*time.Nanosecond)).
		Add(strings.Repeat("a", 2<<5), document.NewBoolValue(true)).
		Add(strings.Repeat("a", 2<<10), document.NewBoolValue(true)).
		Add(strings.Repeat("a", 2<<20), document.NewBoolValue(true))

	data, err := EncodeDocument(doc)
	require.NoError(t, err)

	d := EncodedDocument(data)

	v, err := d.GetByField("age")
	require.NoError(t, err)
	require.Equal(t, document.NewInt64Value(10), v)

	v, err = d.GetByField("address")
	require.NoError(t, err)
	require.Equal(t, document.NewNullValue(), v)

	v, err = d.GetByField("name")
	require.NoError(t, err)
	require.Equal(t, document.NewTextValue("john"), v)

	v, err = d.GetByField("doesnexists")
	require.Equal(t, err, document.ErrFieldNotFound)

	v, err = d.GetByField("d")
	require.NoError(t, err)
	require.Equal(t, document.NewDurationValue(10*time.Nanosecond), v)

	v, err = d.GetByField(strings.Repeat("a", 2<<20))
	require.NoError(t, err)
	require.Equal(t, document.NewBoolValue(true), v)
}

func TestGetByIndex(t *testing.T) {
	arr := document.NewValueBuffer().
		Append(document.NewInt64Value(10)).
		Append(document.NewNullValue()).
		Append(document.NewTextValue("john")).
		Append(document.NewDurationValue(10 * time.Nanosecond))

	data, err := EncodeArray(arr)
	require.NoError(t, err)

	a := EncodedArray(data)

	v, err := a.GetByIndex(0)
	require.NoError(t, err)
	require.Equal(t, document.NewInt64Value(10), v)

	v, err = a.GetByIndex(1)
	require.NoError(t, err)
	require.Equal(t, document.NewNullValue(), v)

	v, err = a.GetByIndex(2)
	require.NoError(t, err)
	require.Equal(t, document.NewTextValue("john"), v)

	v, err = a.GetByIndex(1000)
	require.Equal(t, err, document.ErrValueNotFound)
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
			`[10, "john", {"city": "Ajaccio", "country": "France"}, [11]]` + "\n",
		},
		{
			"Empty array",
			document.NewValueBuffer(),
			"[]\n",
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

func BenchmarkEncodeDocumentJSON(b *testing.B) {
	m := make(map[string]int64)

	for i := int64(0); i < 100; i++ {
		m[fmt.Sprintf("name-%d", i)] = i
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		json.Marshal(m)
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

func BenchmarkDocumentIterate(b *testing.B) {
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

func BenchmarkDecodeDocumentJSON(b *testing.B) {
	m := make(map[string]int64)

	for i := int64(0); i < 100; i++ {
		m[fmt.Sprintf("name-%d", i)] = i
	}

	d, err := json.Marshal(m)
	require.NoError(b, err)

	mm := make(map[string]int64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		json.Unmarshal(d, &mm)
	}
}
