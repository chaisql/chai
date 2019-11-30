package document_test

import (
	"bytes"
	"testing"

	"github.com/asdine/genji/document"
	"github.com/stretchr/testify/require"
)

var _ document.Document = new(document.FieldBuffer)

func TestFieldBuffer(t *testing.T) {
	buf := document.NewFieldBuffer(
		document.NewInt64Field("a", 10),
		document.NewStringField("b", "hello"),
	)

	t.Run("Iterate", func(t *testing.T) {
		var i int
		err := buf.Iterate(func(f document.Field) error {
			require.NotEmpty(t, f)
			require.Equal(t, f, buf[i])
			i++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, i)
	})

	t.Run("Add", func(t *testing.T) {
		var buf document.FieldBuffer

		buf.Add(document.NewInt64Field("a", 10))
		buf.Add(document.NewStringField("b", "hello"))

		c := document.NewBoolField("c", true)
		buf.Add(c)
		require.Len(t, buf, 3)
		require.Equal(t, buf[2], c)
	})

	t.Run("ScanRecord", func(t *testing.T) {
		buf1 := document.NewFieldBuffer(
			document.NewInt64Field("a", 10),
			document.NewStringField("b", "hello"),
		)

		buf2 := document.NewFieldBuffer(
			document.NewInt64Field("a", 20),
			document.NewStringField("b", "bye"),
			document.NewBoolField("c", true),
		)

		err := buf1.ScanRecord(buf2)
		require.NoError(t, err)

		require.Equal(t, document.NewFieldBuffer(
			document.NewInt64Field("a", 10),
			document.NewStringField("b", "hello"),
			document.NewInt64Field("a", 20),
			document.NewStringField("b", "bye"),
			document.NewBoolField("c", true),
		), buf1)
	})

	t.Run("GetValueByName", func(t *testing.T) {
		f, err := buf.GetValueByName("a")
		require.NoError(t, err)
		require.Equal(t, document.NewInt64Field("a", 10), f)

		f, err = buf.GetValueByName("not existing")
		require.Error(t, err)
		require.Zero(t, f)
	})

	t.Run("Set", func(t *testing.T) {
		buf1 := document.NewFieldBuffer(
			document.NewInt64Field("a", 10),
			document.NewStringField("b", "hello"),
		)

		buf1.Set(document.NewInt64Field("a", 11))
		require.Equal(t, document.NewInt64Field("a", 11), buf1[0])

		buf1.Set(document.NewInt64Field("c", 12))
		require.Len(t, buf1, 3)
		require.Equal(t, document.NewInt64Field("c", 12), buf1[2])
	})

	t.Run("Delete", func(t *testing.T) {
		buf1 := document.NewFieldBuffer(
			document.NewInt64Field("a", 10),
			document.NewStringField("b", "hello"),
		)

		err := buf1.Delete("a")
		require.NoError(t, err)
		require.Len(t, buf1, 1)
		require.Equal(t, document.NewFieldBuffer(
			document.NewStringField("b", "hello"),
		), buf1)

		err = buf1.Delete("b")
		require.NoError(t, err)
		require.Len(t, buf1, 0)

		err = buf1.Delete("b")
		require.Error(t, err)
	})

	t.Run("Replace", func(t *testing.T) {
		buf1 := document.NewFieldBuffer(
			document.NewInt64Field("a", 10),
			document.NewStringField("b", "hello"),
		)

		err := buf1.Replace("a", document.NewInt64Field("c", 10))
		require.NoError(t, err)
		require.Equal(t, document.NewFieldBuffer(
			document.NewInt64Field("c", 10),
			document.NewStringField("b", "hello"),
		), buf1)

		err = buf1.Replace("d", document.NewInt64Field("c", 11))
		require.Error(t, err)
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

		err := rec.Iterate(func(f document.Field) error {
			counter[f.Name]++
			v, err := f.Decode()
			require.NoError(t, err)
			require.Equal(t, m[f.Name], v)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, counter, 3)
		require.Equal(t, counter["Name"], 1)
		require.Equal(t, counter["Age"], 1)
		require.Equal(t, counter["NilField"], 1)
	})

	t.Run("Field", func(t *testing.T) {
		f, err := rec.GetValueByName("Name")
		require.NoError(t, err)
		require.Equal(t, document.Field{Name: "Name", Value: document.Value{Type: document.String, Data: []byte("foo")}}, f)

		f, err = rec.GetValueByName("Age")
		require.NoError(t, err)
		require.Equal(t, document.Field{Name: "Age", Value: document.Value{Type: document.Int, Data: document.EncodeInt(10)}}, f)

		f, err = rec.GetValueByName("NilField")
		require.NoError(t, err)
		require.Equal(t, document.Field{Name: "NilField", Value: document.Value{Type: document.Null}}, f)

		_, err = rec.GetValueByName("bar")
		require.Error(t, err)
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
			document.FieldBuffer([]document.Field{
				document.NewStringField("name", "John"),
				document.NewUint16Field("age", 10),
			}),
			`{"name":"John","age":10}` + "\n",
		},
		{
			"Nested",
			document.FieldBuffer([]document.Field{
				document.NewStringField("name", "John"),
				document.NewUint16Field("age", 10),
				document.NewObjectField("address", document.FieldBuffer([]document.Field{
					document.NewStringField("city", "Ajaccio"),
					document.NewStringField("country", "France"),
				})),
			}),
			`{"name":"John","age":10,"address":{"city":"Ajaccio","country":"France"}}` + "\n",
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
	r := document.FieldBuffer([]document.Field{
		document.NewBytesField("a", []byte("foo")),
		document.NewStringField("b", "bar"),
		document.NewBoolField("c", true),
		document.NewUintField("d", 10),
		document.NewUint8Field("e", 10),
		document.NewUint16Field("f", 10),
		document.NewUint32Field("g", 10),
		document.NewUint64Field("h", 10),
		document.NewIntField("i", 10),
		document.NewInt8Field("j", 10),
		document.NewInt16Field("k", 10),
		document.NewInt32Field("l", 10),
		document.NewInt64Field("m", 10),
		document.NewFloat64Field("n", 10.5),
	})

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

	t.Run("RecordScanner", func(t *testing.T) {
		var rs recordScanner
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

type recordScanner struct {
	fn func(r document.Document) error
}

func (rs recordScanner) ScanRecord(r document.Document) error {
	return rs.fn(r)
}
