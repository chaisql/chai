package row_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/types"
)

var _ row.Row = new(row.ColumnBuffer)

func TestColumnBuffer(t *testing.T) {
	var buf row.ColumnBuffer
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
		require.NoError(t, err)
		require.Equal(t, 2, i)
	})

	t.Run("Add", func(t *testing.T) {
		var buf row.ColumnBuffer
		buf.Add("a", types.NewIntegerValue(10))
		buf.Add("b", types.NewTextValue("hello"))

		c := types.NewBooleanValue(true)
		buf.Add("c", c)
		require.Equal(t, 3, buf.Len())
	})

	t.Run("ScanRow", func(t *testing.T) {
		var buf1, buf2 row.ColumnBuffer

		buf1.Add("a", types.NewIntegerValue(10))
		buf1.Add("b", types.NewTextValue("hello"))

		buf2.Add("a", types.NewIntegerValue(20))
		buf2.Add("b", types.NewTextValue("bye"))
		buf2.Add("c", types.NewBooleanValue(true))

		err := buf1.ScanRow(&buf2)
		require.NoError(t, err)

		var buf row.ColumnBuffer
		buf.Add("a", types.NewIntegerValue(10))
		buf.Add("b", types.NewTextValue("hello"))
		buf.Add("a", types.NewIntegerValue(20))
		buf.Add("b", types.NewTextValue("bye"))
		buf.Add("c", types.NewBooleanValue(true))
		require.Equal(t, buf, buf1)
	})

	t.Run("Get", func(t *testing.T) {
		v, err := buf.Get("a")
		require.NoError(t, err)
		require.Equal(t, types.NewIntegerValue(10), v)

		v, err = buf.Get("not existing")
		require.ErrorIs(t, err, types.ErrColumnNotFound)
		require.Zero(t, v)
	})

	t.Run("Set", func(t *testing.T) {
		tests := []struct {
			name   string
			data   string
			column string
			value  types.Value
			want   string
			fails  bool
		}{
			{"root", `{}`, `a`, types.NewIntegerValue(1), `{"a": 1}`, false},
			{"add column", `{"a": 1}`, `c`, types.NewTextValue("foo"), `{"a": 1, "c": "foo"}`, false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var fb row.ColumnBuffer

				r := testutil.MakeRow(t, tt.data)
				err := fb.Copy(r)
				require.NoError(t, err)
				err = fb.Set(tt.column, tt.value)
				if tt.fails {
					require.Error(t, err)
					return
				}

				require.NoError(t, err)
				data, err := row.MarshalJSON(&fb)
				require.NoError(t, err)
				require.Equal(t, tt.want, string(data))
			})
		}
	})

	t.Run("Delete", func(t *testing.T) {
		tests := []struct {
			object   string
			column   string
			expected string
			fails    bool
		}{
			{`{"a": 10, "b": "hello"}`, "a", `{"b": "hello"}`, false},
			{`{"a": 10, "b": "hello"}`, "c", ``, true},
		}

		for _, test := range tests {
			t.Run(test.object, func(t *testing.T) {
				var buf row.ColumnBuffer
				err := buf.Copy(testutil.MakeRow(t, test.object))
				require.NoError(t, err)

				err = buf.Delete(test.column)
				if test.fails {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					got, err := json.Marshal(&buf)
					require.NoError(t, err)
					require.JSONEq(t, test.expected, string(got))
				}
			})
		}
	})

	t.Run("Replace", func(t *testing.T) {
		var buf row.ColumnBuffer
		buf.Add("a", types.NewIntegerValue(10))
		buf.Add("b", types.NewTextValue("hello"))

		err := buf.Replace("a", types.NewBooleanValue(true))
		require.NoError(t, err)
		v, err := buf.Get("a")
		require.NoError(t, err)
		require.Equal(t, types.NewBooleanValue(true), v)
		err = buf.Replace("d", types.NewIntegerValue(11))
		require.Error(t, err)
	})

	t.Run("Apply", func(t *testing.T) {
		d := testutil.MakeRow(t, `{
			"a": "b",
			"c": "d",
			"e": "f"
		}`)

		buf := row.NewColumnBuffer()
		err := buf.Copy(d)
		require.NoError(t, err)

		err = buf.Apply(func(c string, v types.Value) (types.Value, error) {
			return types.NewIntegerValue(1), nil
		})
		require.NoError(t, err)

		got, err := json.Marshal(buf)
		require.NoError(t, err)
		require.JSONEq(t, `{"a":1, "c":1, "e":1}`, string(got))
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

		// nil pointers must be skipped
		// otherwise they must be dereferenced
		P *int
		Q *int

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

	t.Run("Iterate", func(t *testing.T) {
		doc, err := row.NewFromStruct(u)
		require.NoError(t, err)

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
				require.EqualValues(t, *u.Q, types.AsInt64(v))
			case 15:
				require.EqualValues(t, u.Z, types.AsInt64(v))
			case 16:
				require.EqualValues(t, types.TypeNull, v.Type())
			case 17:
				require.EqualValues(t, types.TypeBigint, v.Type())
			case 18:
				require.EqualValues(t, types.TypeTimestamp, v.Type())
			case 19:
			default:
				require.FailNowf(t, "", "unknown field %q", f)
			}

			counter++

			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 19, counter)
	})

	t.Run("Get", func(t *testing.T) {
		doc, err := row.NewFromStruct(u)
		require.NoError(t, err)

		v, err := doc.Get("a")
		require.NoError(t, err)
		require.Equal(t, u.A, types.AsByteSlice(v))
		v, err = doc.Get("b")
		require.NoError(t, err)
		require.Equal(t, u.B, types.AsString(v))
		v, err = doc.Get("c")
		require.NoError(t, err)
		require.Equal(t, u.C, types.AsBool(v))
		v, err = doc.Get("la-reponse-d")
		require.NoError(t, err)
		require.EqualValues(t, u.D, types.AsInt64(v))
		v, err = doc.Get("e")
		require.NoError(t, err)
		require.EqualValues(t, u.E, types.AsInt64(v))
		v, err = doc.Get("f")
		require.NoError(t, err)
		require.EqualValues(t, u.F, types.AsInt64(v))
		v, err = doc.Get("g")
		require.NoError(t, err)
		require.EqualValues(t, u.G, types.AsInt64(v))
		v, err = doc.Get("h")
		require.NoError(t, err)
		require.EqualValues(t, u.H, types.AsInt64(v))
		v, err = doc.Get("i")
		require.NoError(t, err)
		require.EqualValues(t, u.I, types.AsInt64(v))
		v, err = doc.Get("j")
		require.NoError(t, err)
		require.EqualValues(t, u.J, types.AsInt64(v))
		v, err = doc.Get("k")
		require.NoError(t, err)
		require.EqualValues(t, u.K, types.AsInt64(v))
		v, err = doc.Get("l")
		require.NoError(t, err)
		require.EqualValues(t, u.L, types.AsInt64(v))
		v, err = doc.Get("m")
		require.NoError(t, err)
		require.EqualValues(t, u.M, types.AsInt64(v))
		v, err = doc.Get("n")
		require.NoError(t, err)
		require.Equal(t, u.N, types.AsFloat64(v))

		v, err = doc.Get("bb")
		require.NoError(t, err)
		var tm time.Time
		require.NoError(t, row.ScanValue(v, &tm))
		require.Equal(t, u.BB, tm)
	})

	t.Run("pointers", func(t *testing.T) {
		type s struct {
			A *int
		}

		d, err := row.NewFromStruct(new(s))
		require.NoError(t, err)
		_, err = d.Get("a")
		require.ErrorIs(t, err, types.ErrColumnNotFound)

		a := 10
		ss := s{A: &a}
		d, err = row.NewFromStruct(&ss)
		require.NoError(t, err)
		v, err := d.Get("a")
		require.NoError(t, err)
		require.Equal(t, types.NewBigintValue(10), v)
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

	err = fn("b", types.NewBigintValue(f.B))
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

func (f *foo) Get(field string) (types.Value, error) {
	switch field {
	case "a":
		return types.NewTextValue(f.A), nil
	case "b":
		return types.NewBigintValue(f.B), nil
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
		o        row.Row
		expected string
	}{
		{
			"Flat",
			row.NewColumnBuffer().
				Add("name", types.NewTextValue("John")).
				Add("age", types.NewIntegerValue(10)).
				Add(`"something with" quotes`, types.NewIntegerValue(10)),
			`{"\"something with\" quotes":10,"age":10,"name":"John"}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := json.Marshal(test.o)
			require.NoError(t, err)
			require.Equal(t, test.expected, string(data))
			require.NoError(t, err)
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
			_ = f.Iterate(func(string, types.Value) error {
				return nil
			})
		}
	})

}
