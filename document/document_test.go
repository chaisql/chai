package document_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/asdine/genji/document"
	"github.com/stretchr/testify/require"
)

var _ document.Document = new(document.FieldBuffer)

func TestFieldBuffer(t *testing.T) {
	var buf document.FieldBuffer
	buf.Add("a", document.NewInt64Value(10))
	buf.Add("b", document.NewStringValue("hello"))

	t.Run("Iterate", func(t *testing.T) {
		var i int
		err := buf.Iterate(func(f string, v document.Value) error {
			switch i {
			case 0:
				require.Equal(t, "a", f)
				require.Equal(t, document.NewInt64Value(10), v)
			case 1:
				require.Equal(t, "b", f)
				require.Equal(t, document.NewStringValue("hello"), v)
			}
			i++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, i)
	})

	t.Run("Add", func(t *testing.T) {
		var buf document.FieldBuffer
		buf.Add("a", document.NewInt64Value(10))
		buf.Add("b", document.NewStringValue("hello"))

		c := document.NewBoolValue(true)
		buf.Add("c", c)
		require.Equal(t, 3, buf.Len())
	})

	t.Run("ScanDocument", func(t *testing.T) {
		var buf1, buf2 document.FieldBuffer

		buf1.Add("a", document.NewInt64Value(10))
		buf1.Add("b", document.NewStringValue("hello"))

		buf2.Add("a", document.NewInt64Value(20))
		buf2.Add("b", document.NewStringValue("bye"))
		buf2.Add("c", document.NewBoolValue(true))

		err := buf1.ScanDocument(buf2)
		require.NoError(t, err)

		var buf document.FieldBuffer
		buf.Add("a", document.NewInt64Value(10))
		buf.Add("b", document.NewStringValue("hello"))
		buf.Add("a", document.NewInt64Value(20))
		buf.Add("b", document.NewStringValue("bye"))
		buf.Add("c", document.NewBoolValue(true))
		require.Equal(t, buf, buf1)
	})

	t.Run("GetByField", func(t *testing.T) {
		v, err := buf.GetByField("a")
		require.NoError(t, err)
		require.Equal(t, document.NewInt64Value(10), v)

		v, err = buf.GetByField("not existing")
		require.Equal(t, document.ErrFieldNotFound, err)
		require.Zero(t, v)
	})

	t.Run("Set", func(t *testing.T) {
		var buf document.FieldBuffer
		buf.Add("a", document.NewInt64Value(10))
		buf.Add("b", document.NewStringValue("hello"))

		buf.Set("a", document.NewFloat64Value(11))
		v, err := buf.GetByField("a")
		require.NoError(t, err)
		require.Equal(t, document.NewFloat64Value(11), v)

		buf.Set("c", document.NewInt64Value(12))
		require.Equal(t, 3, buf.Len())
		v, err = buf.GetByField("c")
		require.NoError(t, err)
		require.Equal(t, document.NewInt64Value(12), v)
	})

	t.Run("Delete", func(t *testing.T) {
		var buf document.FieldBuffer
		buf.Add("a", document.NewInt64Value(10))
		buf.Add("b", document.NewStringValue("hello"))

		err := buf.Delete("a")
		require.NoError(t, err)
		require.Equal(t, 1, buf.Len())
		v, _ := buf.GetByField("b")
		require.Equal(t, document.NewStringValue("hello"), v)
		_, err = buf.GetByField("a")
		require.Error(t, err)

		err = buf.Delete("b")
		require.NoError(t, err)
		require.Equal(t, 0, buf.Len())

		err = buf.Delete("b")
		require.Error(t, err)
	})

	t.Run("Replace", func(t *testing.T) {
		var buf document.FieldBuffer
		buf.Add("a", document.NewInt64Value(10))
		buf.Add("b", document.NewStringValue("hello"))

		err := buf.Replace("a", document.NewBoolValue(true))
		require.NoError(t, err)
		v, err := buf.GetByField("a")
		require.NoError(t, err)
		require.Equal(t, document.NewBoolValue(true), v)
		err = buf.Replace("d", document.NewInt64Value(11))
		require.Error(t, err)
	})

	t.Run("UnmarshalJSON", func(t *testing.T) {
		tests := []struct {
			name     string
			data     string
			expected *document.FieldBuffer
			fails    bool
		}{
			{"empty object", "{}", document.NewFieldBuffer(), false},
			{"classic object", `{"a": 1, "b": true, "c": "hello", "d": [1, 2, 3], "e": {"f": "g"}}`,
				document.NewFieldBuffer().
					Add("a", document.NewInt8Value(1)).
					Add("b", document.NewBoolValue(true)).
					Add("c", document.NewStringValue("hello")).
					Add("d", document.NewArrayValue(document.NewValueBuffer().
						Append(document.NewInt8Value(1)).
						Append(document.NewInt8Value(2)).
						Append(document.NewInt8Value(3)))).
					Add("e", document.NewDocumentValue(document.NewFieldBuffer().Add("f", document.NewStringValue("g")))),
				false},
			{"string values", `{"a": "hello ciao"}`, document.NewFieldBuffer().Add("a", document.NewStringValue("hello ciao")), false},
			{"+int8 values", `{"a": 1}`, document.NewFieldBuffer().Add("a", document.NewInt8Value(1)), false},
			{"-int8 values", `{"a": -1}`, document.NewFieldBuffer().Add("a", document.NewInt8Value(-1)), false},
			{"+int16 values", `{"a": 1000}`, document.NewFieldBuffer().Add("a", document.NewInt16Value(1000)), false},
			{"-int16 values", `{"a": 1000}`, document.NewFieldBuffer().Add("a", document.NewInt16Value(1000)), false},
			{"+int32 values", `{"a": 1000000}`, document.NewFieldBuffer().Add("a", document.NewInt32Value(1000000)), false},
			{"-int32 values", `{"a": 1000000}`, document.NewFieldBuffer().Add("a", document.NewInt32Value(1000000)), false},
			{"+int64 values", `{"a": 10000000000}`, document.NewFieldBuffer().Add("a", document.NewInt64Value(10000000000)), false},
			{"-int64 values", `{"a": -10000000000}`, document.NewFieldBuffer().Add("a", document.NewInt64Value(-10000000000)), false},
			{"uint64 values", `{"a": 10000000000000000000}`, document.NewFieldBuffer().Add("a", document.NewUint64Value(10000000000000000000)), false},
			{"+float64 values", `{"a": 10000000000.0}`, document.NewFieldBuffer().Add("a", document.NewFloat64Value(10000000000)), false},
			{"-float64 values", `{"a": -10000000000.0}`, document.NewFieldBuffer().Add("a", document.NewFloat64Value(-10000000000)), false},
			{"bool values", `{"a": true, "b": false}`, document.NewFieldBuffer().Add("a", document.NewBoolValue(true)).Add("b", document.NewBoolValue(false)), false},
			{"empty arrays", `{"a": []}`, document.NewFieldBuffer().Add("a", document.NewArrayValue(document.NewValueBuffer())), false},
			{"nested arrays", `{"a": [[1,  2]]}`, document.NewFieldBuffer().
				Add("a", document.NewArrayValue(
					document.NewValueBuffer().
						Append(document.NewArrayValue(
							document.NewValueBuffer().
								Append(document.NewInt8Value(1)).
								Append(document.NewInt8Value(2)))))), false},
			{"missing comma", `{"a": 1 "b": 2}`, nil, true},
			{"missing closing brackets", `{"a": 1, "b": 2`, nil, true},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				var buf document.FieldBuffer

				err := json.Unmarshal([]byte(test.data), &buf)
				if test.fails {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, *test.expected, buf)
				}
			})
		}
	})
}

func TestNewFromMap(t *testing.T) {
	m := map[string]interface{}{
		"Name":     "foo",
		"Age":      10,
		"NilField": nil,
	}

	rec := document.NewFromMap(m)

	t.Run("Iterate", func(t *testing.T) {
		counter := make(map[string]int)

		err := rec.Iterate(func(f string, v document.Value) error {
			counter[f]++
			x, err := v.Decode()
			require.NoError(t, err)
			require.Equal(t, m[f], x)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, counter, 3)
		require.Equal(t, counter["Name"], 1)
		require.Equal(t, counter["Age"], 1)
		require.Equal(t, counter["NilField"], 1)
	})

	t.Run("GetByField", func(t *testing.T) {
		v, err := rec.GetByField("Name")
		require.NoError(t, err)
		require.Equal(t, document.Value{Type: document.StringValue, Data: []byte("foo")}, v)

		v, err = rec.GetByField("Age")
		require.NoError(t, err)
		require.Equal(t, document.Value{Type: document.IntValue, Data: document.EncodeInt(10)}, v)

		v, err = rec.GetByField("NilField")
		require.NoError(t, err)
		require.Equal(t, document.Value{Type: document.NullValue}, v)

		_, err = rec.GetByField("bar")
		require.Equal(t, document.ErrFieldNotFound, err)
	})
}

func TestToJSON(t *testing.T) {
	tests := []struct {
		name     string
		r        document.Document
		expected string
	}{
		{
			"Flat",
			document.NewFieldBuffer().
				Add("name", document.NewStringValue("John")).
				Add("age", document.NewUint16Value(10)),
			`{"name":"John","age":10}` + "\n",
		},
		{
			"Nested",
			document.NewFieldBuffer().
				Add("name", document.NewStringValue("John")).
				Add("age", document.NewUint16Value(10)).
				Add("address", document.NewDocumentValue(document.NewFieldBuffer().
					Add("city", document.NewStringValue("Ajaccio")).
					Add("country", document.NewStringValue("France")),
				)).
				Add("friends", document.NewArrayValue(
					document.NewValueBuffer().
						Append(document.NewStringValue("fred")).
						Append(document.NewStringValue("jamie")),
				)),
			`{"name":"John","age":10,"address":{"city":"Ajaccio","country":"France"},"friends":["fred","jamie"]}` + "\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := document.ToJSON(&buf, test.r)
			require.NoError(t, err)
			require.Equal(t, test.expected, buf.String())
			require.NoError(t, err)
		})
	}
}

func TestScan(t *testing.T) {
	r := document.NewFieldBuffer().
		Add("a", document.NewBytesValue([]byte("foo"))).
		Add("b", document.NewStringValue("bar")).
		Add("c", document.NewBoolValue(true)).
		Add("d", document.NewUintValue(10)).
		Add("e", document.NewUint8Value(10)).
		Add("f", document.NewUint16Value(10)).
		Add("g", document.NewUint32Value(10)).
		Add("h", document.NewUint64Value(10)).
		Add("i", document.NewIntValue(10)).
		Add("j", document.NewInt8Value(10)).
		Add("k", document.NewInt16Value(10)).
		Add("l", document.NewInt32Value(10)).
		Add("m", document.NewInt64Value(10)).
		Add("n", document.NewFloat64Value(10.5))

	var a []byte
	var b string
	var c bool
	var d uint
	var e uint8
	var f uint16
	var g uint32
	var h uint64
	var i int
	var j int8
	var k int16
	var l int32
	var m int64
	var n float64

	err := document.Scan(r, &a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m, &n)
	require.NoError(t, err)
	require.Equal(t, a, []byte("foo"))
	require.Equal(t, b, "bar")
	require.Equal(t, c, true)
	require.Equal(t, d, uint(10))
	require.Equal(t, e, uint8(10))
	require.Equal(t, f, uint16(10))
	require.Equal(t, g, uint32(10))
	require.Equal(t, h, uint64(10))
	require.Equal(t, i, int(10))
	require.Equal(t, j, int8(10))
	require.Equal(t, k, int16(10))
	require.Equal(t, l, int32(10))
	require.Equal(t, m, int64(10))
	require.Equal(t, n, float64(10.5))

	t.Run("DocumentScanner", func(t *testing.T) {
		var rs documentScanner
		rs.fn = func(rr document.Document) error {
			require.Equal(t, r, rr)
			return nil
		}
		err := document.Scan(r, &rs)
		require.NoError(t, err)
	})

	t.Run("Map", func(t *testing.T) {
		m := make(map[string]interface{})
		err := document.Scan(r, m)
		require.NoError(t, err)
		require.Len(t, m, 14)
	})

	t.Run("MapPtr", func(t *testing.T) {
		var m map[string]interface{}
		err := document.Scan(r, &m)
		require.NoError(t, err)
		require.Len(t, m, 14)
	})
}

type documentScanner struct {
	fn func(r document.Document) error
}

func (rs documentScanner) ScanDocument(r document.Document) error {
	return rs.fn(r)
}
