package document_test

import (
	"testing"

	"github.com/asdine/genji/document"
	"github.com/stretchr/testify/require"
)

func TestScan(t *testing.T) {
	doc := document.NewFieldBuffer().
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
		Add("n", document.NewFloat64Value(10.5)).
		Add("o", document.NewArrayValue(
			document.NewValueBuffer().
				Append(document.NewBoolValue(true)),
		)).
		Add("p", document.NewDocumentValue(
			document.NewFieldBuffer().
				Add("foo", document.NewStringValue("foo")).
				Add("bar", document.NewStringValue("bar")),
		)).
		Add("q", document.NewDocumentValue(
			document.NewFieldBuffer().
				Add("foo", document.NewStringValue("foo")).
				Add("bar", document.NewStringValue("bar")),
		)).
		Add("r", document.NewDocumentValue(
			document.NewFieldBuffer().
				Add("foo", document.NewStringValue("foo")).
				Add("bar", document.NewStringValue("bar")).
				Add("baz", document.NewStringValue("baz")).
				Add("-", document.NewStringValue("bat")),
		)).
		Add("s", document.NewDocumentValue(
			document.NewFieldBuffer().
				Add("foo", document.NewStringValue("foo")).
				Add("bar", document.NewStringValue("bar")),
		))

	type foo struct {
		Foo string
		Pub *string `genji:"bar"`
		Baz *string `genji:"-"`
	}

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
	var o []bool
	var p foo
	var q *foo = new(foo)
	var r *foo
	var s map[string]string

	err := document.Scan(doc, &a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m, &n, &o, &p, &q, &r, &s)
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
	require.Equal(t, o, []bool{true})
	bar := "bar"
	require.Equal(t, foo{Foo: "foo", Pub: &bar}, p)
	require.Equal(t, &foo{Foo: "foo", Pub: &bar}, q)
	require.Equal(t, &foo{Foo: "foo", Pub: &bar}, r)
	require.Equal(t, map[string]string{"foo": "foo", "bar": "bar"}, s)

	t.Run("DocumentScanner", func(t *testing.T) {
		var ds documentScanner
		ds.fn = func(d document.Document) error {
			require.Equal(t, doc, d)
			return nil
		}
		err := document.StructScan(doc, &ds)
		require.NoError(t, err)
	})

	t.Run("Map", func(t *testing.T) {
		m := make(map[string]interface{})
		err := document.MapScan(doc, m)
		require.NoError(t, err)
		require.Len(t, m, 19)
	})

	t.Run("MapPtr", func(t *testing.T) {
		var m map[string]interface{}
		err := document.MapScan(doc, &m)
		require.NoError(t, err)
		require.Len(t, m, 19)
	})

	t.Run("Small Slice", func(t *testing.T) {
		s := make([]int, 1)
		arr := document.NewValueBuffer().Append(document.NewInt16Value(1)).Append(document.NewInt16Value(2))
		err := document.SliceScan(arr, &s)
		require.NoError(t, err)
		require.Len(t, s, 2)
		require.Equal(t, []int{1, 2}, s)
	})

	t.Run("Slice overwrite", func(t *testing.T) {
		s := make([]int, 1)
		arr := document.NewValueBuffer().Append(document.NewInt16Value(1)).Append(document.NewInt16Value(2))
		err := document.SliceScan(arr, &s)
		require.NoError(t, err)
		err = document.SliceScan(arr, &s)
		require.NoError(t, err)
		require.Len(t, s, 2)
		require.Equal(t, []int{1, 2}, s)
	})
}

type documentScanner struct {
	fn func(d document.Document) error
}

func (ds documentScanner) ScanDocument(d document.Document) error {
	return ds.fn(d)
}
