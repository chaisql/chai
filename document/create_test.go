package document_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/stretchr/testify/require"
)

func TestNewFromJSON(t *testing.T) {
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
				Add("a", document.NewIntegerValue(1)).
				Add("b", document.NewBoolValue(true)).
				Add("c", document.NewTextValue("hello")).
				Add("d", document.NewArrayValue(document.NewValueBuffer().
					Append(document.NewIntegerValue(1)).
					Append(document.NewIntegerValue(2)).
					Append(document.NewIntegerValue(3)))).
				Add("e", document.NewDocumentValue(document.NewFieldBuffer().Add("f", document.NewTextValue("g")))),
			false},
		{"string values", `{"a": "hello ciao"}`, document.NewFieldBuffer().Add("a", document.NewTextValue("hello ciao")), false},
		{"+integer values", `{"a": 1000}`, document.NewFieldBuffer().Add("a", document.NewIntegerValue(1000)), false},
		{"-integer values", `{"a": -1000}`, document.NewFieldBuffer().Add("a", document.NewIntegerValue(-1000)), false},
		{"+float values", `{"a": 10000000000.0}`, document.NewFieldBuffer().Add("a", document.NewDoubleValue(10000000000)), false},
		{"-float values", `{"a": -10000000000.0}`, document.NewFieldBuffer().Add("a", document.NewDoubleValue(-10000000000)), false},
		{"bool values", `{"a": true, "b": false}`, document.NewFieldBuffer().Add("a", document.NewBoolValue(true)).Add("b", document.NewBoolValue(false)), false},
		{"empty arrays", `{"a": []}`, document.NewFieldBuffer().Add("a", document.NewArrayValue(document.NewValueBuffer())), false},
		{"nested arrays", `{"a": [[1,  2]]}`, document.NewFieldBuffer().
			Add("a", document.NewArrayValue(
				document.NewValueBuffer().
					Append(document.NewArrayValue(
						document.NewValueBuffer().
							Append(document.NewIntegerValue(1)).
							Append(document.NewIntegerValue(2)))))), false},
		{"missing comma", `{"a": 1 "b": 2}`, nil, true},
		{"missing closing brackets", `{"a": 1, "b": 2`, nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := document.NewFromJSON([]byte(test.data))

			fb := document.NewFieldBuffer()
			err := fb.Copy(d)

			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, *test.expected, *fb)
			}
		})
	}

	t.Run("GetByField", func(t *testing.T) {
		d := document.NewFromJSON([]byte(`{"a": 1000}`))

		v, err := d.GetByField("a")
		require.NoError(t, err)
		require.Equal(t, document.NewIntegerValue(1000), v)

		v, err = d.GetByField("b")
		require.Equal(t, document.ErrFieldNotFound, err)
	})
}

func TestNewFromMap(t *testing.T) {
	m := map[string]interface{}{
		"name":     "foo",
		"age":      10,
		"nilField": nil,
	}

	doc, err := document.NewFromMap(m)
	require.NoError(t, err)

	t.Run("Iterate", func(t *testing.T) {
		counter := make(map[string]int)

		err := doc.Iterate(func(f string, v document.Value) error {
			counter[f]++
			switch f {
			case "name":
				require.Equal(t, m[f], v.V.(string))
			default:
				require.EqualValues(t, m[f], v.V)
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
		v, err := doc.GetByField("name")
		require.NoError(t, err)
		require.Equal(t, document.NewTextValue("foo"), v)

		v, err = doc.GetByField("age")
		require.NoError(t, err)
		require.Equal(t, document.NewIntegerValue(10), v)

		v, err = doc.GetByField("nilField")
		require.NoError(t, err)
		require.Equal(t, document.NewNullValue(), v)

		_, err = doc.GetByField("bar")
		require.Equal(t, document.ErrFieldNotFound, err)
	})

	t.Run("Invalid types", func(t *testing.T) {

		// test NewFromMap rejects invalid types
		_, err = document.NewFromMap(8)
		require.Error(t, err, "Expected document.NewFromMap to return an error if the passed parameter is not a map")
		_, err = document.NewFromMap(map[int]float64{2: 4.3})
		require.Error(t, err, "Expected document.NewFromMap to return an error if the passed parameter is not a map with a string key type")
	})
}

func TestNewFromStruct(t *testing.T) {
	type group struct {
		Ig int
	}

	type user struct {
		A []byte
		B string
		C bool
		D uint `genji:"la-reponse-d"`
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

		AA int `genji:"-"` // ignored

		*group

		// unexported fields should be ignored
		t int
	}

	u := user{
		A:  []byte("foo"),
		B:  "bar",
		C:  true,
		D:  1,
		E:  2,
		F:  3,
		G:  4,
		H:  5,
		I:  6,
		J:  7,
		K:  8,
		L:  9,
		M:  10,
		N:  11.12,
		Z:  26,
		AA: 27,
		group: &group{
			Ig: 100,
		},
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
			switch counter {
			case 0:
				require.Equal(t, u.A, v.V.([]byte))
			case 1:
				require.Equal(t, u.B, v.V.(string))
			case 2:
				require.Equal(t, u.C, v.V.(bool))
			case 3:
				require.Equal(t, "la-reponse-d", f)
				require.EqualValues(t, u.D, v.V.(int64))
			case 4:
				require.EqualValues(t, u.E, v.V.(int64))
			case 5:
				require.EqualValues(t, u.F, v.V.(int64))
			case 6:
				require.EqualValues(t, u.G, v.V.(int64))
			case 7:
				require.EqualValues(t, u.H, v.V.(int64))
			case 8:
				require.EqualValues(t, u.I, v.V.(int64))
			case 9:
				require.EqualValues(t, u.J, v.V.(int64))
			case 10:
				require.EqualValues(t, u.K, v.V.(int64))
			case 11:
				require.EqualValues(t, u.L, v.V.(int64))
			case 12:
				require.EqualValues(t, u.M, v.V.(int64))
			case 13:
				require.Equal(t, u.N, v.V.(float64))
			case 14:
				require.EqualValues(t, document.DocumentValue, v.Type)
			case 15:
				require.EqualValues(t, document.NullValue, v.Type)
			case 16:
				require.EqualValues(t, *u.Q, v.V.(int64))
			case 17:
				require.EqualValues(t, document.DocumentValue, v.Type)
			case 18:
				require.EqualValues(t, document.NullValue, v.Type)
			case 19:
				require.EqualValues(t, document.ArrayValue, v.Type)
			case 20:
				require.EqualValues(t, document.NullValue, v.Type)
			case 21:
				require.EqualValues(t, document.ArrayValue, v.Type)
			case 22:
				require.EqualValues(t, document.ArrayValue, v.Type)
			case 23:
				require.EqualValues(t, document.ArrayValue, v.Type)
			case 24:
				require.EqualValues(t, document.ArrayValue, v.Type)
			case 25:
				require.EqualValues(t, u.Z, v.V.(int64))
			case 26:
				require.EqualValues(t, document.NullValue, v.Type)
			case 27:
				require.EqualValues(t, document.IntegerValue, v.Type)
			default:
				require.FailNowf(t, "", "unknown field %q", f)
			}

			counter++

			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 28, counter)
	})

	t.Run("GetByField", func(t *testing.T) {
		doc, err := document.NewFromStruct(u)
		require.NoError(t, err)

		v, err := doc.GetByField("a")
		require.NoError(t, err)
		require.Equal(t, u.A, v.V.([]byte))
		v, err = doc.GetByField("b")
		require.NoError(t, err)
		require.Equal(t, u.B, v.V.(string))
		v, err = doc.GetByField("c")
		require.NoError(t, err)
		require.Equal(t, u.C, v.V.(bool))
		v, err = doc.GetByField("la-reponse-d")
		require.NoError(t, err)
		require.EqualValues(t, u.D, v.V.(int64))
		v, err = doc.GetByField("e")
		require.NoError(t, err)
		require.EqualValues(t, u.E, v.V.(int64))
		v, err = doc.GetByField("f")
		require.NoError(t, err)
		require.EqualValues(t, u.F, v.V.(int64))
		v, err = doc.GetByField("g")
		require.NoError(t, err)
		require.EqualValues(t, u.G, v.V.(int64))
		v, err = doc.GetByField("h")
		require.NoError(t, err)
		require.EqualValues(t, u.H, v.V.(int64))
		v, err = doc.GetByField("i")
		require.NoError(t, err)
		require.EqualValues(t, u.I, v.V.(int64))
		v, err = doc.GetByField("j")
		require.NoError(t, err)
		require.EqualValues(t, u.J, v.V.(int64))
		v, err = doc.GetByField("k")
		require.NoError(t, err)
		require.EqualValues(t, u.K, v.V.(int64))
		v, err = doc.GetByField("l")
		require.NoError(t, err)
		require.EqualValues(t, u.L, v.V.(int64))
		v, err = doc.GetByField("m")
		require.NoError(t, err)
		require.EqualValues(t, u.M, v.V.(int64))
		v, err = doc.GetByField("n")
		require.NoError(t, err)
		require.Equal(t, u.N, v.V.(float64))

		v, err = doc.GetByField("o")
		require.NoError(t, err)
		d, ok := v.V.(document.Document)
		require.True(t, ok)
		v, err = d.GetByField("ig")
		require.NoError(t, err)
		require.EqualValues(t, 0, v.V.(int64))

		v, err = doc.GetByField("ig")
		require.NoError(t, err)
		require.EqualValues(t, 100, v.V.(int64))

		v, err = doc.GetByField("t")
		require.NoError(t, err)
		a, ok := v.V.(document.Array)
		require.True(t, ok)
		var count int
		err = a.Iterate(func(i int, v document.Value) error {
			count++
			require.EqualValues(t, i+1, v.V.(int64))
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 3, count)
		v, err = a.GetByIndex(10)
		require.Equal(t, err, document.ErrFieldNotFound)
		v, err = a.GetByIndex(1)
		require.NoError(t, err)
		require.EqualValues(t, 2, v.V.(int64))
	})

}

func BenchmarkJSONToDocument(b *testing.B) {
	data := []byte(`{"_id":"5f8aefb8e443c6c13afdb305","index":0,"guid":"42c2719e-3371-4b2f-b855-d302a8b7eab0","isActive":true,"balance":"$1,064.79","picture":"http://placehold.it/32x32","age":40,"eyeColor":"blue","name":"Adele Webb","gender":"female","company":"EXTRAGEN","email":"adelewebb@extragen.com","phone":"+1 (964) 409-2397","address":"970 Charles Place, Watrous, Texas, 2522","about":"Amet non do ullamco duis velit sunt esse et cillum nisi mollit ea magna. Tempor ut occaecat proident laborum velit nisi et excepteur exercitation non est labore. Laboris pariatur enim proident et. Qui minim enim et incididunt incididunt adipisicing tempor. Occaecat adipisicing sint ex ut exercitation exercitation voluptate. Laboris adipisicing ut cillum eu cillum est sunt amet Lorem quis pariatur.\r\n","registered":"2016-05-25T10:36:44 -04:00","latitude":64.57112,"longitude":176.136138,"tags":["velit","minim","eiusmod","est","eu","voluptate","deserunt"],"friends":[{"id":0,"name":"Mathis Robertson"},{"id":1,"name":"Cecilia Donaldson"},{"id":2,"name":"Joann Goodwin"}],"greeting":"Hello, Adele Webb! You have 2 unread messages.","favoriteFruit":"apple"}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := document.NewFromJSON(data)
		d.Iterate(func(string, document.Value) error {
			return nil
		})
	}
}
