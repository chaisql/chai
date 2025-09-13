package row_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/types"
	"github.com/stretchr/testify/require"
)

func TestNewValue(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name            string
		value, expected interface{}
	}{
		{"bytes", []byte("bar"), []byte("bar")},
		{"string", "bar", "bar"},
		{"bool", true, true},
		{"uint64", uint64(10), int64(10)},
		{"int64", int64(10), int64(10)},
		{"float64", 10.1, float64(10.1)},
		{"null", nil, nil},
		{"time", now, now.UTC()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v, err := row.NewValue(test.value)
			require.NoError(t, err)
			require.Equal(t, test.expected, v.V())
		})
	}
}

func TestNewFromCSV(t *testing.T) {
	headers := []string{"a", "b", "c"}
	columns := []string{"A", "B", "C"}

	r := row.NewFromCSV(headers, columns)
	v, err := r.Get("a")
	require.NoError(t, err)
	require.Equal(t, "A", types.AsString(v))
	v, err = r.Get("b")
	require.NoError(t, err)
	require.Equal(t, "B", types.AsString(v))
	v, err = r.Get("c")
	require.NoError(t, err)
	require.Equal(t, "C", types.AsString(v))
}

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
}
