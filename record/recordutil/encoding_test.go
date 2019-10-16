package recordutil_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/asdine/genji/record"
	"github.com/asdine/genji/record/recordutil"
	"github.com/stretchr/testify/require"
)

func TestIteratorToCSV(t *testing.T) {
	tests := []struct {
		name     string
		expected string
	}{
		{"OK", `John 0,10
John 1,11
John 2,12
`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var records []record.Record

			for i := 0; i < 3; i++ {
				records = append(records, record.FieldBuffer([]record.Field{
					record.NewStringField("name", fmt.Sprintf("John %d", i)),
					record.NewIntField("age", 10+i),
				}))
			}

			var buf bytes.Buffer
			err := recordutil.IteratorToCSV(&buf, record.NewStream(record.NewIterator(records...)))
			require.NoError(t, err)
			require.Equal(t, test.expected, buf.String())
			require.NoError(t, err)
		})
	}
}

func TestRecordToJSON(t *testing.T) {
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
			err := recordutil.RecordToJSON(&buf, r)
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

	err := recordutil.Scan(r, &a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m, &n, &o)
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
}
