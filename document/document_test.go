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
			{"empty object, missing closing bracket", "{", nil, true},
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
		"name":     "foo",
		"age":      10,
		"nilField": nil,
	}

	rec := document.NewFromMap(m)

	t.Run("Iterate", func(t *testing.T) {
		counter := make(map[string]int)

		err := rec.Iterate(func(f string, v document.Value) error {
			counter[f]++
			switch f {
			case "name":
				require.Equal(t, m[f], string(v.V.([]byte)))
			default:
				require.Equal(t, m[f], v.V)
			}
			return nil
		})
		require.NoError(t, err)
		require.Len(t, counter, 3)
		require.Equal(t, counter["name"], 1)
		require.Equal(t, counter["age"], 1)
		require.Equal(t, counter["nilField"], 1)
	})

	t.Run("GetByField", func(t *testing.T) {
		v, err := rec.GetByField("name")
		require.NoError(t, err)
		require.Equal(t, document.NewStringValue("foo"), v)

		v, err = rec.GetByField("age")
		require.NoError(t, err)
		require.Equal(t, document.NewIntValue(10), v)

		v, err = rec.GetByField("nilField")
		require.NoError(t, err)
		require.Equal(t, document.NewNullValue(), v)

		_, err = rec.GetByField("bar")
		require.Equal(t, document.ErrFieldNotFound, err)
	})
}

func TestNewFromStruct(t *testing.T) {
	type group struct {
		A int
	}

	type user struct {
		A []byte
		B string
		C bool
		D uint
		E uint8
		F uint16
		G uint32
		H uint64
		I int
		J int8
		K int16
		L int32
		M int64
		N float64
		// structs must be considered as documents
		O group

		// nil pointers must be considered as Null values
		// otherwise they must be dereferenced
		P *int
		Q *int

		// struct pointers should be considered as documents
		// if there are nil though, the value must be Null
		R *group
		S *group

		T  []int
		U  []int
		V  []*int
		W  []user
		X  []interface{}
		Y  [3]int
		Z  interface{}
		ZZ interface{}

		// embedded fields are not supported currently, they should be ignored
		*group

		// unexported fields should be ignored
		t int
	}

	u := user{
		A: []byte("foo"),
		B: "bar",
		C: true,
		D: 1,
		E: 2,
		F: 3,
		G: 4,
		H: 5,
		I: 6,
		J: 7,
		K: 8,
		L: 9,
		M: 10,
		N: 11.12,
		Z: 26,
	}

	q := 5
	u.Q = &q
	u.R = new(group)
	u.T = []int{1, 2, 3}
	u.V = []*int{&q}
	u.W = []user{u}
	u.X = []interface{}{1, "foo"}

	t.Run("Iterate", func(t *testing.T) {
		doc, err := document.NewFromStruct(u)
		require.NoError(t, err)

		var counter int

		err = doc.Iterate(func(f string, v document.Value) error {
			counter++
			switch f {
			case "A":
				require.Equal(t, u.A, v.V.([]byte))
			case "B":
				require.Equal(t, u.B, string(v.V.([]byte)))
			case "C":
				require.Equal(t, u.C, v.V.(bool))
			case "D":
				require.Equal(t, u.D, v.V.(uint))
			case "E":
				require.Equal(t, u.E, v.V.(uint8))
			case "F":
				require.Equal(t, u.F, v.V.(uint16))
			case "G":
				require.Equal(t, u.G, v.V.(uint32))
			case "H":
				require.Equal(t, u.H, v.V.(uint64))
			case "I":
				require.Equal(t, u.I, v.V.(int))
			case "J":
				require.Equal(t, u.J, v.V.(int8))
			case "K":
				require.Equal(t, u.K, v.V.(int16))
			case "L":
				require.Equal(t, u.L, v.V.(int32))
			case "M":
				require.Equal(t, u.M, v.V.(int64))
			case "N":
				require.Equal(t, u.N, v.V.(float64))
			case "O":
				require.Equal(t, document.DocumentValue, v.Type)
			case "P":
				require.Equal(t, document.NullValue, v.Type)
			case "Q":
				require.Equal(t, *u.Q, v.V.(int))
			case "R":
				require.Equal(t, document.DocumentValue, v.Type)
			case "S":
				require.Equal(t, document.NullValue, v.Type)
			case "T":
				require.Equal(t, document.ArrayValue, v.Type)
			case "U":
				require.Equal(t, document.NullValue, v.Type)
			case "V":
				require.Equal(t, document.ArrayValue, v.Type)
			case "W":
				require.Equal(t, document.ArrayValue, v.Type)
			case "X":
				require.Equal(t, document.ArrayValue, v.Type)
			case "Y":
				require.Equal(t, document.ArrayValue, v.Type)
			case "Z":
				require.Equal(t, u.Z, v.V.(int))
			case "ZZ":
				require.Equal(t, document.NullValue, v.Type)
			}
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 27, counter)
	})

	t.Run("GetByField", func(t *testing.T) {
		doc, err := document.NewFromStruct(u)
		require.NoError(t, err)

		v, err := doc.GetByField("A")
		require.NoError(t, err)
		require.Equal(t, u.A, v.V.([]byte))
		v, err = doc.GetByField("B")
		require.NoError(t, err)
		require.Equal(t, u.B, string(v.V.([]byte)))
		v, err = doc.GetByField("C")
		require.NoError(t, err)
		require.Equal(t, u.C, v.V.(bool))
		v, err = doc.GetByField("D")
		require.NoError(t, err)
		require.Equal(t, u.D, v.V.(uint))
		v, err = doc.GetByField("E")
		require.NoError(t, err)
		require.Equal(t, u.E, v.V.(uint8))
		v, err = doc.GetByField("F")
		require.NoError(t, err)
		require.Equal(t, u.F, v.V.(uint16))
		v, err = doc.GetByField("G")
		require.NoError(t, err)
		require.Equal(t, u.G, v.V.(uint32))
		v, err = doc.GetByField("H")
		require.NoError(t, err)
		require.Equal(t, u.H, v.V.(uint64))
		v, err = doc.GetByField("I")
		require.NoError(t, err)
		require.Equal(t, u.I, v.V.(int))
		v, err = doc.GetByField("J")
		require.NoError(t, err)
		require.Equal(t, u.J, v.V.(int8))
		v, err = doc.GetByField("K")
		require.NoError(t, err)
		require.Equal(t, u.K, v.V.(int16))
		v, err = doc.GetByField("L")
		require.NoError(t, err)
		require.Equal(t, u.L, v.V.(int32))
		v, err = doc.GetByField("M")
		require.NoError(t, err)
		require.Equal(t, u.M, v.V.(int64))
		v, err = doc.GetByField("N")
		require.NoError(t, err)
		require.Equal(t, u.N, v.V.(float64))

		v, err = doc.GetByField("O")
		require.NoError(t, err)
		d, err := v.ConvertToDocument()
		require.NoError(t, err)
		v, err = d.GetByField("A")
		require.NoError(t, err)
		require.Equal(t, 0, v.V.(int))

		v, err = doc.GetByField("T")
		require.NoError(t, err)
		a, err := v.ConvertToArray()
		require.NoError(t, err)
		var count int
		err = a.Iterate(func(i int, v document.Value) error {
			count++
			require.Equal(t, i+1, v.V.(int))
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 3, count)
		v, err = a.GetByIndex(10)
		require.Equal(t, err, document.ErrFieldNotFound)
		v, err = a.GetByIndex(1)
		require.NoError(t, err)
		require.Equal(t, 2, v.V.(int))
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
		var ds documentScanner
		ds.fn = func(d document.Document) error {
			require.Equal(t, r, d)
			return nil
		}
		err := document.Scan(r, &ds)
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
	fn func(d document.Document) error
}

func (ds documentScanner) ScanDocument(d document.Document) error {
	return ds.fn(d)
}
