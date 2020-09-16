package document_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding/msgpack"
	"github.com/stretchr/testify/require"
)

func TestScan(t *testing.T) {
	now := time.Now()

	simpleDoc := document.NewFieldBuffer().
		Add("foo", document.NewTextValue("foo")).
		Add("bar", document.NewTextValue("bar")).
		Add("baz", document.NewArrayValue(document.NewValueBuffer(
			document.NewIntegerValue(10),
			document.NewDoubleValue(20.5),
		)))

	nestedDoc := document.NewFieldBuffer().
		Add("foo", document.NewDocumentValue(simpleDoc))

	var buf bytes.Buffer
	codec := msgpack.NewCodec()
	err := codec.NewEncoder(&buf).EncodeDocument(nestedDoc)
	require.NoError(t, err)

	doc := document.NewFieldBuffer().
		Add("a", document.NewBlobValue([]byte("foo"))).
		Add("b", document.NewTextValue("bar")).
		Add("c", document.NewBoolValue(true)).
		Add("d", document.NewIntegerValue(10)).
		Add("e", document.NewIntegerValue(10)).
		Add("f", document.NewIntegerValue(10)).
		Add("g", document.NewIntegerValue(10)).
		Add("h", document.NewIntegerValue(10)).
		Add("i", document.NewDoubleValue(10.5)).
		Add("j", document.NewArrayValue(
			document.NewValueBuffer().
				Append(document.NewBoolValue(true)),
		)).
		Add("k", document.NewDocumentValue(
			document.NewFieldBuffer().
				Add("foo", document.NewTextValue("foo")).
				Add("bar", document.NewTextValue("bar")),
		)).
		Add("l", document.NewDocumentValue(
			document.NewFieldBuffer().
				Add("foo", document.NewTextValue("foo")).
				Add("bar", document.NewTextValue("bar")),
		)).
		Add("m", document.NewDocumentValue(
			document.NewFieldBuffer().
				Add("foo", document.NewTextValue("foo")).
				Add("bar", document.NewTextValue("bar")).
				Add("baz", document.NewTextValue("baz")).
				Add("-", document.NewTextValue("bat")),
		)).
		Add("n", document.NewDocumentValue(
			document.NewFieldBuffer().
				Add("foo", document.NewTextValue("foo")).
				Add("bar", document.NewTextValue("bar")),
		)).
		Add("o", document.NewDurationValue(10*time.Nanosecond)).
		Add("p", document.NewTextValue(now.Format(time.RFC3339Nano))).
		Add("r", document.NewDocumentValue(codec.NewDocument(buf.Bytes())))

	type foo struct {
		Foo string
		Pub *string `genji:"bar"`
		Baz *string `genji:"-"`
	}

	var a []byte
	var b string
	var c bool
	var d int
	var e int8
	var f int16
	var g int32
	var h int64
	var i float64
	var j []bool
	var k foo
	var l *foo = new(foo)
	var m *foo
	var n map[string]string
	var o time.Duration
	var p time.Time
	var r map[string]interface{}

	err = document.Scan(doc, &a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m, &n, &o, &p, &r)
	require.NoError(t, err)
	require.Equal(t, a, []byte("foo"))
	require.Equal(t, b, "bar")
	require.Equal(t, c, true)
	require.Equal(t, d, int(10))
	require.Equal(t, e, int8(10))
	require.Equal(t, f, int16(10))
	require.Equal(t, g, int32(10))
	require.Equal(t, h, int64(10))
	require.Equal(t, i, float64(10.5))
	require.Equal(t, j, []bool{true})
	bar := "bar"
	require.Equal(t, foo{Foo: "foo", Pub: &bar}, k)
	require.Equal(t, &foo{Foo: "foo", Pub: &bar}, l)
	require.Equal(t, &foo{Foo: "foo", Pub: &bar}, m)
	require.Equal(t, map[string]string{"foo": "foo", "bar": "bar"}, n)
	require.Equal(t, 10*time.Nanosecond, o)
	require.Equal(t, now.Format(time.RFC3339Nano), p.Format(time.RFC3339Nano))
	require.Equal(t, map[string]interface{}{
		"foo": map[string]interface{}{
			"foo": "foo",
			"bar": "bar",
			"baz": []interface{}{
				int64(10), float64(20.5),
			},
		},
	}, r)

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
		require.Len(t, m, 17)
	})

	t.Run("MapPtr", func(t *testing.T) {
		var m map[string]interface{}
		err := document.MapScan(doc, &m)
		require.NoError(t, err)
		require.Len(t, m, 17)
	})

	t.Run("Small Slice", func(t *testing.T) {
		s := make([]int, 1)
		arr := document.NewValueBuffer().Append(document.NewIntegerValue(1)).Append(document.NewIntegerValue(2))
		err := document.SliceScan(arr, &s)
		require.NoError(t, err)
		require.Len(t, s, 2)
		require.Equal(t, []int{1, 2}, s)
	})

	t.Run("Slice overwrite", func(t *testing.T) {
		s := make([]int, 1)
		arr := document.NewValueBuffer().Append(document.NewIntegerValue(1)).Append(document.NewIntegerValue(2))
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
