package object_test

import (
	"testing"
	"time"

	"github.com/genjidb/genji/internal/encoding"
	"github.com/genjidb/genji/internal/object"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/internal/types"
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

	simpleDoc := object.NewFieldBuffer().
		Add("foo", types.NewTextValue("foo")).
		Add("bar", types.NewTextValue("bar")).
		Add("baz", types.NewArrayValue(object.NewValueBuffer(
			types.NewIntegerValue(10),
			types.NewDoubleValue(20.5),
		)))

	nestedDoc := object.NewFieldBuffer().
		Add("foo", types.NewObjectValue(simpleDoc))

	var buf []byte
	buf, err := encoding.EncodeValue(buf, types.NewObjectValue(nestedDoc), false)
	assert.NoError(t, err)

	dec, _ := encoding.DecodeValue(buf, false)
	assert.NoError(t, err)

	doc := object.NewFieldBuffer().
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
			object.NewValueBuffer().
				Append(types.NewBoolValue(true)),
		)).
		Add("k", types.NewObjectValue(
			object.NewFieldBuffer().
				Add("foo", types.NewTextValue("foo")).
				Add("bar", types.NewTextValue("bar")),
		)).
		Add("l", types.NewObjectValue(
			object.NewFieldBuffer().
				Add("foo", types.NewTextValue("foo")).
				Add("bar", types.NewTextValue("bar")),
		)).
		Add("m", types.NewObjectValue(
			object.NewFieldBuffer().
				Add("foo", types.NewTextValue("foo")).
				Add("bar", types.NewTextValue("bar")).
				Add("baz", types.NewTextValue("baz")).
				Add("-", types.NewTextValue("bat")),
		)).
		Add("n", types.NewObjectValue(
			object.NewFieldBuffer().
				Add("foo", types.NewTextValue("foo")).
				Add("bar", types.NewTextValue("bar")),
		)).
		Add("o", types.NewNullValue()).
		Add("p", types.NewTextValue(now.Format(time.RFC3339Nano))).
		Add("r", dec).
		Add("s", types.NewArrayValue(object.NewValueBuffer(types.NewBoolValue(true), types.NewBoolValue(false)))).
		Add("u", types.NewArrayValue(object.NewValueBuffer(
			types.NewObjectValue(
				object.NewFieldBuffer().
					Add("foo", types.NewTextValue("a")).
					Add("bar", types.NewTextValue("b")),
			),
			types.NewObjectValue(
				object.NewFieldBuffer().
					Add("foo", types.NewTextValue("c")).
					Add("bar", types.NewTextValue("d")),
			),
		))).
		Add("v", types.NewArrayValue(object.NewValueBuffer(
			types.NewObjectValue(
				object.NewFieldBuffer().
					Add("foo", types.NewTextValue("a")).
					Add("bar", types.NewTextValue("b")),
			),
			types.NewObjectValue(
				object.NewFieldBuffer().
					Add("foo", types.NewTextValue("c")).
					Add("bar", types.NewTextValue("d")),
			),
		))).
		Add("w", types.NewArrayValue(object.NewValueBuffer(
			types.NewIntegerValue(1),
			types.NewIntegerValue(2),
			types.NewIntegerValue(3),
			types.NewIntegerValue(4),
		))).
		Add("x", types.NewBlobValue([]byte{1, 2, 3, 4})).
		Add("y", types.NewObjectValue(
			object.NewFieldBuffer().
				Add("foo", types.NewTextValue("foo")).
				Add("bar", types.NewTextValue("bar")).
				Add("baz", types.NewTextValue("baz")).
				Add("bat", types.NewTextValue("bat")).
				Add("-", types.NewTextValue("bat")),
		)).
		Add("z", types.NewTimestampValue(now))

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
	var y struct {
		foo
		Pub string `genji:"bar"`
		Bat string
	}
	var z time.Time

	err = object.Scan(doc, &a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m, &n, &o, &p, &r, &s, &u, &v, &w, &x, &y, &z)
	assert.NoError(t, err)
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
	require.Equal(t, now.UTC(), z)

	t.Run("objectcanner", func(t *testing.T) {
		var ds objectScanner
		ds.fn = func(d types.Object) error {
			require.Equal(t, doc, d)
			return nil
		}
		err := object.StructScan(doc, &ds)
		assert.NoError(t, err)
	})

	t.Run("Map", func(t *testing.T) {
		m := make(map[string]interface{})
		err := object.MapScan(doc, m)
		assert.NoError(t, err)
		require.Len(t, m, 24)
	})

	t.Run("MapPtr", func(t *testing.T) {
		var m map[string]interface{}
		err := object.MapScan(doc, &m)
		assert.NoError(t, err)
		require.Len(t, m, 24)
	})

	t.Run("Small Slice", func(t *testing.T) {
		s := make([]int, 1)
		arr := object.NewValueBuffer().Append(types.NewIntegerValue(1)).Append(types.NewIntegerValue(2))
		err := object.SliceScan(arr, &s)
		assert.NoError(t, err)
		require.Len(t, s, 2)
		require.Equal(t, []int{1, 2}, s)
	})

	t.Run("Slice overwrite", func(t *testing.T) {
		s := make([]int, 1)
		arr := object.NewValueBuffer().Append(types.NewIntegerValue(1)).Append(types.NewIntegerValue(2))
		err := object.SliceScan(arr, &s)
		assert.NoError(t, err)
		err = object.SliceScan(arr, &s)
		assert.NoError(t, err)
		require.Len(t, s, 2)
		require.Equal(t, []int{1, 2}, s)
	})

	t.Run("pointers", func(t *testing.T) {
		type bar struct {
			A *int
		}

		b := bar{}

		d := object.NewFieldBuffer().Add("a", types.NewIntegerValue(10))
		err := object.StructScan(d, &b)
		assert.NoError(t, err)

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

		d := object.NewFieldBuffer().Add("a", types.NewNullValue())
		err := object.StructScan(d, &b)
		assert.NoError(t, err)
		require.Equal(t, bar{}, b)
	})

	t.Run("Incompatible type", func(t *testing.T) {
		var a struct {
			A int
		}

		d := object.NewFieldBuffer().Add("a", types.NewObjectValue(doc))
		err := object.StructScan(d, &a)
		assert.Error(t, err)
	})

	t.Run("Interface member", func(t *testing.T) {
		type foo struct {
			A interface{}
		}

		type bar struct {
			B int
		}

		var f foo
		f.A = &bar{}

		d := object.NewFieldBuffer().Add("a", types.NewObjectValue(object.NewFieldBuffer().Add("b", types.NewIntegerValue(10))))
		err := object.StructScan(d, &f)
		assert.NoError(t, err)
		require.Equal(t, &foo{A: &bar{B: 10}}, &f)
	})

	t.Run("Pointer not to struct", func(t *testing.T) {
		var b int
		d := object.NewFieldBuffer().Add("a", types.NewIntegerValue(10))
		err := object.StructScan(d, &b)
		assert.Error(t, err)
	})
}

type objectScanner struct {
	fn func(d types.Object) error
}

func (ds objectScanner) ScanObject(d types.Object) error {
	return ds.fn(d)
}
