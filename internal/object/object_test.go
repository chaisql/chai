package object_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/chaisql/chai/internal/object"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/chaisql/chai/internal/types"
)

var _ types.Object = new(object.FieldBuffer)

func TestFieldBuffer(t *testing.T) {
	var buf object.FieldBuffer
	buf.Add("a", types.NewIntegerValue(10))
	buf.Add("b", types.NewTextValue("hello"))

	t.Run("Iterate", func(t *testing.T) {
		var i int
		err := buf.Iterate(func(f string, v types.Value) error {
			switch i {
			case 0:
				require.Equal(t, "a", f)
				require.Equal(t, types.NewIntegerValue(10), v)
			case 1:
				require.Equal(t, "b", f)
				require.Equal(t, types.NewTextValue("hello"), v)
			}
			i++
			return nil
		})
		assert.NoError(t, err)
		require.Equal(t, 2, i)
	})

	t.Run("Add", func(t *testing.T) {
		var buf object.FieldBuffer
		buf.Add("a", types.NewIntegerValue(10))
		buf.Add("b", types.NewTextValue("hello"))

		c := types.NewBooleanValue(true)
		buf.Add("c", c)
		require.Equal(t, 3, buf.Len())
	})

	t.Run("ScanObject", func(t *testing.T) {
		var buf1, buf2 object.FieldBuffer

		buf1.Add("a", types.NewIntegerValue(10))
		buf1.Add("b", types.NewTextValue("hello"))

		buf2.Add("a", types.NewIntegerValue(20))
		buf2.Add("b", types.NewTextValue("bye"))
		buf2.Add("c", types.NewBooleanValue(true))

		err := buf1.ScanObject(&buf2)
		assert.NoError(t, err)

		var buf object.FieldBuffer
		buf.Add("a", types.NewIntegerValue(10))
		buf.Add("b", types.NewTextValue("hello"))
		buf.Add("a", types.NewIntegerValue(20))
		buf.Add("b", types.NewTextValue("bye"))
		buf.Add("c", types.NewBooleanValue(true))
		require.Equal(t, buf, buf1)
	})

	t.Run("GetByField", func(t *testing.T) {
		v, err := buf.GetByField("a")
		assert.NoError(t, err)
		require.Equal(t, types.NewIntegerValue(10), v)

		v, err = buf.GetByField("not existing")
		assert.ErrorIs(t, err, types.ErrFieldNotFound)
		require.Zero(t, v)
	})

	t.Run("Set", func(t *testing.T) {
		tests := []struct {
			name  string
			data  string
			path  string
			value types.Value
			want  string
			fails bool
		}{
			{"root", `{}`, `a`, types.NewIntegerValue(1), `{"a": 1}`, false},
			{"add field", `{"a": {"b": [1, 2, 3]}}`, `c`, types.NewTextValue("foo"), `{"a": {"b": [1, 2, 3]}, "c": "foo"}`, false},
			{"non existing doc", `{}`, `a.b.c`, types.NewTextValue("foo"), ``, true},
			{"wrong type", `{"a": 1}`, `a.b.c`, types.NewTextValue("foo"), ``, true},
			{"nested doc", `{"a": "foo"}`, `a`, types.NewObjectValue(object.NewFieldBuffer().
				Add("b", types.NewArrayValue(object.NewValueBuffer().
					Append(types.NewIntegerValue(1)).
					Append(types.NewIntegerValue(2)).
					Append(types.NewIntegerValue(3))))), `{"a": {"b": [1, 2, 3]}}`, false},
			{"nested doc", `{"a": {"b": [1, 2, 3]}}`, `a.b`, types.NewArrayValue(object.NewValueBuffer().
				Append(types.NewIntegerValue(1)).
				Append(types.NewIntegerValue(2)).
				Append(types.NewIntegerValue(3))), `{"a": {"b": [1, 2, 3]}}`, false},
			{"nested array", `{"a": {"b": [1, 2, 3]}}`, `a.b[1]`, types.NewIntegerValue(1), `{"a": {"b": [1, 1, 3]}}`, false},
			{"nested array multiple indexes", `{"a": {"b": [1, 2, [1, 2, {"c": "foo"}]]}}`, `a.b[2][2].c`, types.NewTextValue("bar"), `{"a": {"b": [1, 2, [1, 2, {"c": "bar"}]]}}`, false},
			{"number field", `{"a": {"0": [1, 2, 3]}}`, "a.`0`[0]", types.NewIntegerValue(6), `{"a": {"0": [6, 2, 3]}}`, false},
			{"object in array", `{"a": [{"b":"foo"}, 2, 3]}`, `a[0].b`, types.NewTextValue("bar"), `{"a": [{"b": "bar"}, 2, 3]}`, false},
			// with errors or request ignored doc unchanged
			{"field not found", `{"a": {"b": [1, 2, 3]}}`, `a.b.c`, types.NewIntegerValue(1), `{"a": {"b": [1, 2, 3]}}`, false},
			{"unknown path", `{"a": {"b": [1, 2, 3]}}`, `a.e.f`, types.NewIntegerValue(1), ``, true},
			{"index out of range", `{"a": {"b": [1, 2, 3]}}`, `a.b[1000]`, types.NewIntegerValue(1), ``, true},
			{"object not array", `{"a": {"b": "foo"}}`, `a[0].b`, types.NewTextValue("bar"), ``, true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var fb object.FieldBuffer

				d := object.NewFromJSON([]byte(tt.data))
				err := fb.Copy(d)
				assert.NoError(t, err)
				p, err := parser.ParsePath(tt.path)
				assert.NoError(t, err)
				err = fb.Set(p, tt.value)
				if tt.fails {
					assert.Error(t, err)
					return
				}

				assert.NoError(t, err)
				data, err := object.MarshalJSON(&fb)
				assert.NoError(t, err)
				require.Equal(t, tt.want, string(data))
			})
		}
	})

	t.Run("Delete", func(t *testing.T) {
		tests := []struct {
			object     string
			deletePath string
			expected   string
			fails      bool
		}{
			{`{"a": 10, "b": "hello"}`, "a", `{"b": "hello"}`, false},
			{`{"a": 10, "b": "hello"}`, "c", ``, true},
			{`{"a": [1], "b": "hello"}`, "a[0]", `{"a": [], "b": "hello"}`, false},
			{`{"a": [1, 2], "b": "hello"}`, "a[0]", `{"a": [2], "b": "hello"}`, false},
			{`{"a": [1, 2], "b": "hello"}`, "a[5]", ``, true},
			{`{"a": [1, {"c": [1]}], "b": "hello"}`, "a[1].c", `{"a": [1, {}], "b": "hello"}`, false},
			{`{"a": [1, {"c": [1]}], "b": "hello"}`, "a[1].d", ``, true},
		}

		for _, test := range tests {
			t.Run(test.object, func(t *testing.T) {
				var buf object.FieldBuffer
				err := json.Unmarshal([]byte(test.object), &buf)
				assert.NoError(t, err)

				path := testutil.ParseObjectPath(t, test.deletePath)

				err = buf.Delete(path)
				if test.fails {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					got, err := json.Marshal(&buf)
					assert.NoError(t, err)
					require.JSONEq(t, test.expected, string(got))
				}
			})
		}
	})

	t.Run("Replace", func(t *testing.T) {
		var buf object.FieldBuffer
		buf.Add("a", types.NewIntegerValue(10))
		buf.Add("b", types.NewTextValue("hello"))

		err := buf.Replace("a", types.NewBooleanValue(true))
		assert.NoError(t, err)
		v, err := buf.GetByField("a")
		assert.NoError(t, err)
		require.Equal(t, types.NewBooleanValue(true), v)
		err = buf.Replace("d", types.NewIntegerValue(11))
		assert.Error(t, err)
	})

	t.Run("Apply", func(t *testing.T) {
		d := object.NewFromJSON([]byte(`{
			"a": "b",
			"c": ["d", "e"],
			"f": {"g": "h"}
		}`))

		buf := object.NewFieldBuffer()
		err := buf.Copy(d)
		assert.NoError(t, err)

		err = buf.Apply(func(p object.Path, v types.Value) (types.Value, error) {
			if v.Type() == types.TypeArray || v.Type() == types.TypeObject {
				return v, nil
			}

			return types.NewIntegerValue(1), nil
		})
		assert.NoError(t, err)

		got, err := json.Marshal(buf)
		assert.NoError(t, err)
		require.JSONEq(t, `{"a": 1, "c": [1, 1], "f": {"g": 1}}`, string(got))
	})

	t.Run("CloneValue", func(t *testing.T) {
		d := testutil.MakeObject(t, `{
			"a": "b",
			"c": ["d", "e"],
			"f": {"g": "h"}
		}`)

		got, err := object.CloneValue(types.NewObjectValue(d))
		require.NoError(t, err)
		testutil.RequireObjEqual(t, d, types.AsObject(got))
	})

	t.Run("UnmarshalJSON", func(t *testing.T) {
		tests := []struct {
			name     string
			data     string
			expected *object.FieldBuffer
			fails    bool
		}{
			{"empty object", "{}", object.NewFieldBuffer(), false},
			{"empty object, missing closing bracket", "{", nil, true},
			{"classic object", `{"a": 1, "b": true, "c": "hello", "d": [1, 2, 3], "e": {"f": "g"}}`,
				object.NewFieldBuffer().
					Add("a", types.NewIntegerValue(1)).
					Add("b", types.NewBooleanValue(true)).
					Add("c", types.NewTextValue("hello")).
					Add("d", types.NewArrayValue(object.NewValueBuffer().
						Append(types.NewIntegerValue(1)).
						Append(types.NewIntegerValue(2)).
						Append(types.NewIntegerValue(3)))).
					Add("e", types.NewObjectValue(object.NewFieldBuffer().Add("f", types.NewTextValue("g")))),
				false},
			{"string values", `{"a": "hello ciao"}`, object.NewFieldBuffer().Add("a", types.NewTextValue("hello ciao")), false},
			{"+integer values", `{"a": 1000}`, object.NewFieldBuffer().Add("a", types.NewIntegerValue(1000)), false},
			{"-integer values", `{"a": -1000}`, object.NewFieldBuffer().Add("a", types.NewIntegerValue(-1000)), false},
			{"+float values", `{"a": 10000000000.0}`, object.NewFieldBuffer().Add("a", types.NewDoubleValue(10000000000)), false},
			{"-float values", `{"a": -10000000000.0}`, object.NewFieldBuffer().Add("a", types.NewDoubleValue(-10000000000)), false},
			{"bool values", `{"a": true, "b": false}`, object.NewFieldBuffer().Add("a", types.NewBooleanValue(true)).Add("b", types.NewBooleanValue(false)), false},
			{"empty arrays", `{"a": []}`, object.NewFieldBuffer().Add("a", types.NewArrayValue(object.NewValueBuffer())), false},
			{"nested arrays", `{"a": [[1,  2]]}`, object.NewFieldBuffer().
				Add("a", types.NewArrayValue(
					object.NewValueBuffer().
						Append(types.NewArrayValue(
							object.NewValueBuffer().
								Append(types.NewIntegerValue(1)).
								Append(types.NewIntegerValue(2)))))), false},
			{"missing comma", `{"a": 1 "b": 2}`, nil, true},
			{"missing closing brackets", `{"a": 1, "b": 2`, nil, true},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				var buf object.FieldBuffer

				err := json.Unmarshal([]byte(test.data), &buf)
				if test.fails {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					require.Equal(t, *test.expected, buf)
				}
			})
		}
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
		D uint `chai:"la-reponse-d"`
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
		// structs must be considered as objects
		O group

		// nil pointers must be skipped
		// otherwise they must be dereferenced
		P *int
		Q *int

		// struct pointers should be considered as objects
		// if there are nil though, they must be skipped
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

		AA int `chai:"-"` // ignored

		*group

		BB time.Time // some have special encoding as object

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
		BB: time.Date(2020, 11, 15, 16, 37, 10, 20, time.UTC),
		t:  99,
	}

	q := 5
	u.Q = &q
	u.R = new(group)
	u.T = []int{1, 2, 3}
	u.V = []*int{&q}
	u.W = []user{u}
	u.X = []interface{}{1, "foo"}

	t.Run("Iterate", func(t *testing.T) {
		doc, err := object.NewFromStruct(u)
		assert.NoError(t, err)

		var counter int

		err = doc.Iterate(func(f string, v types.Value) error {
			switch counter {
			case 0:
				require.Equal(t, u.A, types.AsByteSlice(v))
			case 1:
				require.Equal(t, u.B, types.AsString(v))
			case 2:
				require.Equal(t, u.C, types.AsBool(v))
			case 3:
				require.Equal(t, "la-reponse-d", f)
				require.EqualValues(t, u.D, types.AsInt64(v))
			case 4:
				require.EqualValues(t, u.E, types.AsInt64(v))
			case 5:
				require.EqualValues(t, u.F, types.AsInt64(v))
			case 6:
				require.EqualValues(t, u.G, types.AsInt64(v))
			case 7:
				require.EqualValues(t, u.H, types.AsInt64(v))
			case 8:
				require.EqualValues(t, u.I, types.AsInt64(v))
			case 9:
				require.EqualValues(t, u.J, types.AsInt64(v))
			case 10:
				require.EqualValues(t, u.K, types.AsInt64(v))
			case 11:
				require.EqualValues(t, u.L, types.AsInt64(v))
			case 12:
				require.EqualValues(t, u.M, types.AsInt64(v))
			case 13:
				require.Equal(t, u.N, types.AsFloat64(v))
			case 14:
				require.EqualValues(t, types.TypeObject, v.Type())
			case 15:
				require.EqualValues(t, *u.Q, types.AsInt64(v))
			case 16:
				require.EqualValues(t, types.TypeObject, v.Type())
			case 17:
				require.EqualValues(t, types.TypeArray, v.Type())
			case 18:
				require.EqualValues(t, types.TypeNull, v.Type())
			case 19:
				require.EqualValues(t, types.TypeArray, v.Type())
			case 20:
				require.EqualValues(t, types.TypeArray, v.Type())
			case 21:
				require.EqualValues(t, types.TypeArray, v.Type())
			case 22:
				require.EqualValues(t, types.TypeArray, v.Type())
			case 23:
				require.EqualValues(t, u.Z, types.AsInt64(v))
			case 24:
				require.EqualValues(t, types.TypeNull, v.Type())
			case 25:
				require.EqualValues(t, types.TypeInteger, v.Type())
			case 26:
				require.EqualValues(t, types.TypeTimestamp, v.Type())
			default:
				require.FailNowf(t, "", "unknown field %q", f)
			}

			counter++

			return nil
		})
		assert.NoError(t, err)
		require.Equal(t, 27, counter)
	})

	t.Run("GetByField", func(t *testing.T) {
		doc, err := object.NewFromStruct(u)
		assert.NoError(t, err)

		v, err := doc.GetByField("a")
		assert.NoError(t, err)
		require.Equal(t, u.A, types.AsByteSlice(v))
		v, err = doc.GetByField("b")
		assert.NoError(t, err)
		require.Equal(t, u.B, types.AsString(v))
		v, err = doc.GetByField("c")
		assert.NoError(t, err)
		require.Equal(t, u.C, types.AsBool(v))
		v, err = doc.GetByField("la-reponse-d")
		assert.NoError(t, err)
		require.EqualValues(t, u.D, types.AsInt64(v))
		v, err = doc.GetByField("e")
		assert.NoError(t, err)
		require.EqualValues(t, u.E, types.AsInt64(v))
		v, err = doc.GetByField("f")
		assert.NoError(t, err)
		require.EqualValues(t, u.F, types.AsInt64(v))
		v, err = doc.GetByField("g")
		assert.NoError(t, err)
		require.EqualValues(t, u.G, types.AsInt64(v))
		v, err = doc.GetByField("h")
		assert.NoError(t, err)
		require.EqualValues(t, u.H, types.AsInt64(v))
		v, err = doc.GetByField("i")
		assert.NoError(t, err)
		require.EqualValues(t, u.I, types.AsInt64(v))
		v, err = doc.GetByField("j")
		assert.NoError(t, err)
		require.EqualValues(t, u.J, types.AsInt64(v))
		v, err = doc.GetByField("k")
		assert.NoError(t, err)
		require.EqualValues(t, u.K, types.AsInt64(v))
		v, err = doc.GetByField("l")
		assert.NoError(t, err)
		require.EqualValues(t, u.L, types.AsInt64(v))
		v, err = doc.GetByField("m")
		assert.NoError(t, err)
		require.EqualValues(t, u.M, types.AsInt64(v))
		v, err = doc.GetByField("n")
		assert.NoError(t, err)
		require.Equal(t, u.N, types.AsFloat64(v))

		v, err = doc.GetByField("o")
		assert.NoError(t, err)
		d, ok := types.Is[types.Object](v)
		require.True(t, ok)
		v, err = d.GetByField("ig")
		assert.NoError(t, err)
		require.EqualValues(t, 0, types.AsInt64(v))

		v, err = doc.GetByField("ig")
		assert.NoError(t, err)
		require.EqualValues(t, 100, types.AsInt64(v))

		v, err = doc.GetByField("t")
		assert.NoError(t, err)
		a, ok := types.Is[types.Array](v)
		require.True(t, ok)
		var count int
		err = a.Iterate(func(i int, v types.Value) error {
			count++
			require.EqualValues(t, i+1, types.AsInt64(v))
			return nil
		})
		assert.NoError(t, err)
		require.Equal(t, 3, count)
		_, err = a.GetByIndex(10)
		assert.ErrorIs(t, err, types.ErrFieldNotFound)
		v, err = a.GetByIndex(1)
		assert.NoError(t, err)
		require.EqualValues(t, 2, types.AsInt64(v))

		v, err = doc.GetByField("bb")
		assert.NoError(t, err)
		var tm time.Time
		assert.NoError(t, object.ScanValue(v, &tm))
		require.Equal(t, u.BB, tm)
	})

	t.Run("pointers", func(t *testing.T) {
		type s struct {
			A *int
		}

		d, err := object.NewFromStruct(new(s))
		assert.NoError(t, err)
		_, err = d.GetByField("a")
		assert.ErrorIs(t, err, types.ErrFieldNotFound)

		a := 10
		ss := s{A: &a}
		d, err = object.NewFromStruct(&ss)
		assert.NoError(t, err)
		v, err := d.GetByField("a")
		assert.NoError(t, err)
		require.Equal(t, types.NewIntegerValue(10), v)
	})
}

type foo struct {
	A string
	B int64
	C bool
	D float64
}

func (f *foo) Iterate(fn func(field string, value types.Value) error) error {
	var err error

	err = fn("a", types.NewTextValue(f.A))
	if err != nil {
		return err
	}

	err = fn("b", types.NewIntegerValue(f.B))
	if err != nil {
		return err
	}

	err = fn("c", types.NewBooleanValue(f.C))
	if err != nil {
		return err
	}

	err = fn("d", types.NewDoubleValue(f.D))
	if err != nil {
		return err
	}

	return nil
}

func (f *foo) GetByField(field string) (types.Value, error) {
	switch field {
	case "a":
		return types.NewTextValue(f.A), nil
	case "b":
		return types.NewIntegerValue(f.B), nil
	case "c":
		return types.NewBooleanValue(f.C), nil
	case "d":
		return types.NewDoubleValue(f.D), nil
	}

	return nil, errors.New("unknown field")
}

func TestJSONObject(t *testing.T) {
	tests := []struct {
		name     string
		o        types.Object
		expected string
	}{
		{
			"Flat",
			object.NewFieldBuffer().
				Add("name", types.NewTextValue("John")).
				Add("age", types.NewIntegerValue(10)).
				Add(`"something with" quotes`, types.NewIntegerValue(10)),
			`{"name":"John","age":10,"\"something with\" quotes":10}`,
		},
		{
			"Nested",
			object.NewFieldBuffer().
				Add("name", types.NewTextValue("John")).
				Add("age", types.NewIntegerValue(10)).
				Add("address", types.NewObjectValue(object.NewFieldBuffer().
					Add("city", types.NewTextValue("Ajaccio")).
					Add("country", types.NewTextValue("France")),
				)).
				Add("friends", types.NewArrayValue(
					object.NewValueBuffer().
						Append(types.NewTextValue("fred")).
						Append(types.NewTextValue("jamie")),
				)),
			`{"name":"John","age":10,"address":{"city":"Ajaccio","country":"France"},"friends":["fred","jamie"]}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := json.Marshal(test.o)
			assert.NoError(t, err)
			require.Equal(t, test.expected, string(data))
			assert.NoError(t, err)
		})
	}
}

func BenchmarkObjectIterate(b *testing.B) {
	f := foo{
		A: "a",
		B: 1000,
		C: true,
		D: 1e10,
	}

	b.Run("Implementation", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			f.Iterate(func(string, types.Value) error {
				return nil
			})
		}
	})

}
