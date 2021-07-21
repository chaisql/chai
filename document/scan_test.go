package document_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding/msgpack"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func strPtr(s string) *string {
	return &s
}

func boolPtr(b bool) *bool {
	return &b
}

func TestScan(t *testing.T) {
	now := time.Now()

	simpleDoc := document.NewFieldBuffer().
		Add("foo", types.NewTextValue("foo")).
		Add("bar", types.NewTextValue("bar")).
		Add("baz", types.NewArrayValue(document.NewValueBuffer(
			types.NewIntegerValue(10),
			types.NewDoubleValue(20.5),
		)))

	nestedDoc := document.NewFieldBuffer().
		Add("foo", types.NewDocumentValue(simpleDoc))

	var buf bytes.Buffer
	codec := msgpack.NewCodec()
	err := codec.NewEncoder(&buf).EncodeDocument(nestedDoc)
	require.NoError(t, err)

	doc := document.NewFieldBuffer().
		Add("a", types.NewBlobValue([]byte("foo"))).
		Add("b", types.NewTextValue("bar")).
		Add("c", types.NewBoolValue(true)).
		Add("d", types.NewIntegerValue(10)).
		Add("e", types.NewIntegerValue(10)).
		Add("f", types.NewIntegerValue(10)).
		Add("g", types.NewIntegerValue(10)).
		Add("h", types.NewIntegerValue(10)).
		Add("i", types.NewDoubleValue(10.5)).
		Add("j", types.NewArrayValue(
			document.NewValueBuffer().
				Append(types.NewBoolValue(true)),
		)).
		Add("k", types.NewDocumentValue(
			document.NewFieldBuffer().
				Add("foo", types.NewTextValue("foo")).
				Add("bar", types.NewTextValue("bar")),
		)).
		Add("l", types.NewDocumentValue(
			document.NewFieldBuffer().
				Add("foo", types.NewTextValue("foo")).
				Add("bar", types.NewTextValue("bar")),
		)).
		Add("m", types.NewDocumentValue(
			document.NewFieldBuffer().
				Add("foo", types.NewTextValue("foo")).
				Add("bar", types.NewTextValue("bar")).
				Add("baz", types.NewTextValue("baz")).
				Add("-", types.NewTextValue("bat")),
		)).
		Add("n", types.NewDocumentValue(
			document.NewFieldBuffer().
				Add("foo", types.NewTextValue("foo")).
				Add("bar", types.NewTextValue("bar")),
		)).
		Add("o", types.NewNullValue()).
		Add("p", types.NewTextValue(now.Format(time.RFC3339Nano))).
		Add("r", types.NewDocumentValue(codec.NewDecoder(buf.Bytes()))).
		Add("s", types.NewArrayValue(document.NewValueBuffer(types.NewBoolValue(true), types.NewBoolValue(false)))).
		Add("u", types.NewArrayValue(document.NewValueBuffer(
			types.NewDocumentValue(
				document.NewFieldBuffer().
					Add("foo", types.NewTextValue("a")).
					Add("bar", types.NewTextValue("b")),
			),
			types.NewDocumentValue(
				document.NewFieldBuffer().
					Add("foo", types.NewTextValue("c")).
					Add("bar", types.NewTextValue("d")),
			),
		))).
		Add("v", types.NewArrayValue(document.NewValueBuffer(
			types.NewDocumentValue(
				document.NewFieldBuffer().
					Add("foo", types.NewTextValue("a")).
					Add("bar", types.NewTextValue("b")),
			),
			types.NewDocumentValue(
				document.NewFieldBuffer().
					Add("foo", types.NewTextValue("c")).
					Add("bar", types.NewTextValue("d")),
			),
		))).
		Add("w", types.NewArrayValue(document.NewValueBuffer(
			types.NewIntegerValue(1),
			types.NewIntegerValue(2),
			types.NewIntegerValue(3),
			types.NewIntegerValue(4),
		))).
		Add("x", types.NewBlobValue([]byte{1, 2, 3, 4}))

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
	var o []int = []int{1, 2, 3}
	var p time.Time
	var r map[string]interface{}
	var s []*bool
	var u []foo
	var v []*foo
	var w [4]int
	var x [4]uint8

	err = document.Scan(doc, &a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m, &n, &o, &p, &r, &s, &u, &v, &w, &x)
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
	require.Equal(t, foo{Foo: "foo", Pub: strPtr("bar")}, k)
	require.Equal(t, &foo{Foo: "foo", Pub: strPtr("bar")}, l)
	require.Equal(t, &foo{Foo: "foo", Pub: strPtr("bar")}, m)
	require.Equal(t, map[string]string{"foo": "foo", "bar": "bar"}, n)
	require.Equal(t, []int(nil), o)
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
	require.Equal(t, []*bool{boolPtr(true), boolPtr(false)}, s)
	require.Equal(t, foo{Foo: "foo", Pub: strPtr("bar")}, k)
	require.Equal(t, []foo{{Foo: "a", Pub: strPtr("b")}, {Foo: "c", Pub: strPtr("d")}}, u)
	require.Equal(t, []*foo{{Foo: "a", Pub: strPtr("b")}, {Foo: "c", Pub: strPtr("d")}}, v)
	require.Equal(t, [4]int{1, 2, 3, 4}, w)
	require.Equal(t, [4]uint8{1, 2, 3, 4}, x)

	t.Run("DocumentScanner", func(t *testing.T) {
		var ds documentScanner
		ds.fn = func(d types.Document) error {
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
		require.Len(t, m, 22)
	})

	t.Run("MapPtr", func(t *testing.T) {
		var m map[string]interface{}
		err := document.MapScan(doc, &m)
		require.NoError(t, err)
		require.Len(t, m, 22)
	})

	t.Run("Small Slice", func(t *testing.T) {
		s := make([]int, 1)
		arr := document.NewValueBuffer().Append(types.NewIntegerValue(1)).Append(types.NewIntegerValue(2))
		err := document.SliceScan(arr, &s)
		require.NoError(t, err)
		require.Len(t, s, 2)
		require.Equal(t, []int{1, 2}, s)
	})

	t.Run("Slice overwrite", func(t *testing.T) {
		s := make([]int, 1)
		arr := document.NewValueBuffer().Append(types.NewIntegerValue(1)).Append(types.NewIntegerValue(2))
		err := document.SliceScan(arr, &s)
		require.NoError(t, err)
		err = document.SliceScan(arr, &s)
		require.NoError(t, err)
		require.Len(t, s, 2)
		require.Equal(t, []int{1, 2}, s)
	})

	t.Run("pointers", func(t *testing.T) {
		type bar struct {
			A *int
		}

		b := bar{}

		d := document.NewFieldBuffer().Add("a", types.NewIntegerValue(10))
		err := document.StructScan(d, &b)
		require.NoError(t, err)

		a := 10
		require.Equal(t, bar{A: &a}, b)
	})

	t.Run("NULL with pointers", func(t *testing.T) {
		type bar struct {
			A *int
			B *string
			C *int
		}

		c := 10
		b := bar{
			C: &c,
		}

		d := document.NewFieldBuffer().Add("a", types.NewNullValue())
		err := document.StructScan(d, &b)
		require.NoError(t, err)
		require.Equal(t, bar{}, b)
	})
}

type documentScanner struct {
	fn func(d types.Document) error
}

func (ds documentScanner) ScanDocument(d types.Document) error {
	return ds.fn(d)
}
