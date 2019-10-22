package record_test

import (
	"bytes"
	"testing"

	"github.com/asdine/genji/record"
	"github.com/asdine/genji/value"
	"github.com/stretchr/testify/require"
)

var _ record.Record = new(record.FieldBuffer)

func TestFieldBuffer(t *testing.T) {
	buf := record.NewFieldBuffer(
		record.NewInt64Field("a", 10),
		record.NewStringField("b", "hello"),
	)

	t.Run("Iterate", func(t *testing.T) {
		var i int
		err := buf.Iterate(func(f record.Field) error {
			require.NotEmpty(t, f)
			require.Equal(t, f, buf[i])
			i++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, i)
	})

	t.Run("Add", func(t *testing.T) {
		buf := record.NewFieldBuffer(
			record.NewInt64Field("a", 10),
			record.NewStringField("b", "hello"),
		)

		c := record.NewBoolField("c", true)
		buf.Add(c)
		require.Len(t, buf, 3)
		require.Equal(t, buf[2], c)
	})

	t.Run("ScanRecord", func(t *testing.T) {
		buf1 := record.NewFieldBuffer(
			record.NewInt64Field("a", 10),
			record.NewStringField("b", "hello"),
		)

		buf2 := record.NewFieldBuffer(
			record.NewInt64Field("a", 20),
			record.NewStringField("b", "bye"),
			record.NewBoolField("c", true),
		)

		err := buf1.ScanRecord(buf2)
		require.NoError(t, err)

		require.Equal(t, record.NewFieldBuffer(
			record.NewInt64Field("a", 10),
			record.NewStringField("b", "hello"),
			record.NewInt64Field("a", 20),
			record.NewStringField("b", "bye"),
			record.NewBoolField("c", true),
		), buf1)
	})

	t.Run("GetField", func(t *testing.T) {
		f, err := buf.GetField("a")
		require.NoError(t, err)
		require.Equal(t, record.NewInt64Field("a", 10), f)

		f, err = buf.GetField("not existing")
		require.Error(t, err)
		require.Zero(t, f)
	})

	t.Run("Set", func(t *testing.T) {
		buf1 := record.NewFieldBuffer(
			record.NewInt64Field("a", 10),
			record.NewStringField("b", "hello"),
		)

		buf1.Set(record.NewInt64Field("a", 11))
		require.Equal(t, record.NewInt64Field("a", 11), buf1[0])

		buf1.Set(record.NewInt64Field("c", 12))
		require.Len(t, buf1, 3)
		require.Equal(t, record.NewInt64Field("c", 12), buf1[2])
	})

	t.Run("Delete", func(t *testing.T) {
		buf1 := record.NewFieldBuffer(
			record.NewInt64Field("a", 10),
			record.NewStringField("b", "hello"),
		)

		err := buf1.Delete("a")
		require.NoError(t, err)
		require.Len(t, buf1, 1)
		require.Equal(t, record.NewFieldBuffer(
			record.NewStringField("b", "hello"),
		), buf1)

		err = buf1.Delete("b")
		require.NoError(t, err)
		require.Len(t, buf1, 0)

		err = buf1.Delete("b")
		require.Error(t, err)
	})

	t.Run("Replace", func(t *testing.T) {
		buf1 := record.NewFieldBuffer(
			record.NewInt64Field("a", 10),
			record.NewStringField("b", "hello"),
		)

		err := buf1.Replace("a", record.NewInt64Field("c", 10))
		require.NoError(t, err)
		require.Equal(t, record.NewFieldBuffer(
			record.NewInt64Field("c", 10),
			record.NewStringField("b", "hello"),
		), buf1)

		err = buf1.Replace("d", record.NewInt64Field("c", 11))
		require.Error(t, err)
	})
}

func TestNewFromMap(t *testing.T) {
	m := map[string]interface{}{
		"Name":     "foo",
		"Age":      10,
		"NilField": nil,
	}

	rec := record.NewFromMap(m)

	t.Run("Iterate", func(t *testing.T) {
		counter := make(map[string]int)

		err := rec.Iterate(func(f record.Field) error {
			counter[f.Name]++
			v, err := f.Decode()
			require.NoError(t, err)
			require.Equal(t, m[f.Name], v)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, counter, 2)
		require.Equal(t, counter["Name"], 1)
		require.Equal(t, counter["Age"], 1)
	})

	t.Run("Field", func(t *testing.T) {
		f, err := rec.GetField("Name")
		require.NoError(t, err)
		require.Equal(t, record.Field{Name: "Name", Value: value.Value{Type: value.String, Data: []byte("foo")}}, f)

		f, err = rec.GetField("Age")
		require.NoError(t, err)
		require.Equal(t, record.Field{Name: "Age", Value: value.Value{Type: value.Int, Data: value.EncodeInt(10)}}, f)

		_, err = rec.GetField("bar")
		require.Error(t, err)
	})
}

func TestToJSON(t *testing.T) {
	tests := []struct {
		name     string
		expected string
	}{
		{"OK", `{"name":"John","age":10}` + "\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := record.FieldBuffer([]record.Field{
				record.NewStringField("name", "John"),
				record.NewUint16Field("age", 10),
			})

			var buf bytes.Buffer
			err := record.ToJSON(&buf, r)
			require.NoError(t, err)
			require.Equal(t, test.expected, buf.String())
			require.NoError(t, err)
		})
	}
}

func TestScan(t *testing.T) {
	r := record.FieldBuffer([]record.Field{
		record.NewBytesField("a", []byte("foo")),
		record.NewStringField("b", "bar"),
		record.NewBoolField("c", true),
		record.NewUintField("d", 10),
		record.NewUint8Field("e", 10),
		record.NewUint16Field("f", 10),
		record.NewUint32Field("g", 10),
		record.NewUint64Field("h", 10),
		record.NewIntField("i", 10),
		record.NewInt8Field("j", 10),
		record.NewInt16Field("k", 10),
		record.NewInt32Field("l", 10),
		record.NewInt64Field("m", 10),
		record.NewFloat32Field("n", 10.4),
		record.NewFloat64Field("o", 10.5),
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
	var n float32
	var o float64

	err := record.Scan(r, &a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m, &n, &o)
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
	require.Equal(t, n, float32(10.4))
	require.Equal(t, o, float64(10.5))

	t.Run("RecordScanner", func(t *testing.T) {
		var rs recordScanner
		rs.fn = func(rr record.Record) error {
			require.Equal(t, r, rr)
			return nil
		}
		err := record.Scan(r, &rs)
		require.NoError(t, err)
	})

	t.Run("Map", func(t *testing.T) {
		m := make(map[string]interface{})
		err := record.Scan(r, m)
		require.NoError(t, err)
		require.Len(t, m, 15)
	})

	t.Run("MapPtr", func(t *testing.T) {
		var m map[string]interface{}
		err := record.Scan(r, &m)
		require.NoError(t, err)
		require.Len(t, m, 15)
	})
}

type recordScanner struct {
	fn func(r record.Record) error
}

func (rs recordScanner) ScanRecord(r record.Record) error {
	return rs.fn(r)
}
