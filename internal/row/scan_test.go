package row_test

import (
	"testing"
	"time"

	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/chaisql/chai/internal/types"
	"github.com/stretchr/testify/require"
)

func TestScan(t *testing.T) {
	now := time.Now()

	r := row.NewColumnBuffer().
		Add("a", types.NewBlobValue([]byte("foo"))).
		Add("b", types.NewTextValue("bar")).
		Add("c", types.NewBooleanValue(true)).
		Add("d", types.NewIntegerValue(10)).
		Add("e", types.NewIntegerValue(10)).
		Add("f", types.NewIntegerValue(10)).
		Add("g", types.NewIntegerValue(10)).
		Add("h", types.NewIntegerValue(10)).
		Add("i", types.NewDoubleValue(10.5)).
		Add("j", types.NewNullValue()).
		Add("k", types.NewTextValue(now.Format(time.RFC3339Nano))).
		Add("l", types.NewBlobValue([]byte{1, 2, 3, 4})).
		Add("m", types.NewTimestampValue(now))

	var a []byte
	var b string
	var c bool
	var d int
	var e int8
	var f int16
	var g int32
	var h int64
	var i float64
	var j int = 1
	var k time.Time
	var l [4]uint8
	var m time.Time

	err := row.Scan(r, &a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m)
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
	require.Equal(t, 0, j)
	require.Equal(t, now.Format(time.RFC3339Nano), k.Format(time.RFC3339Nano))
	require.Equal(t, [4]uint8{1, 2, 3, 4}, l)
	require.Equal(t, now.UTC(), m)

	t.Run("Map", func(t *testing.T) {
		m := make(map[string]interface{})
		err := row.MapScan(r, m)
		assert.NoError(t, err)
		require.Len(t, m, 13)
	})

	t.Run("MapPtr", func(t *testing.T) {
		var m map[string]interface{}
		err := row.MapScan(r, &m)
		assert.NoError(t, err)
		require.Len(t, m, 13)
	})

	t.Run("pointers", func(t *testing.T) {
		type bar struct {
			A *int
		}

		b := bar{}

		d := row.NewColumnBuffer().Add("a", types.NewIntegerValue(10))
		err := row.StructScan(d, &b)
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

		d := row.NewColumnBuffer().Add("a", types.NewNullValue())
		err := row.StructScan(d, &b)
		assert.NoError(t, err)
		require.Equal(t, bar{}, b)
	})

	t.Run("Incompatible type", func(t *testing.T) {
		var a struct {
			A float64
		}

		d := row.NewColumnBuffer().Add("a", types.NewTimestampValue(time.Now()))
		err := row.StructScan(d, &a)
		assert.Error(t, err)
	})

	t.Run("Pointer not to struct", func(t *testing.T) {
		var b int
		d := row.NewColumnBuffer().Add("a", types.NewIntegerValue(10))
		err := row.StructScan(d, &b)
		assert.Error(t, err)
	})
}
