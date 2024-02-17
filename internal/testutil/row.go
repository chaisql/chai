package testutil

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/chaisql/chai/internal/types"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

func MakeRow(t testing.TB, s string) row.Row {
	var cb row.ColumnBuffer

	err := json.Unmarshal([]byte(s), &cb)
	assert.NoError(t, err)
	return &cb
}

func MakeRows(t testing.TB, s ...string) []row.Row {
	var rows []row.Row
	for _, v := range s {
		rows = append(rows, MakeRow(t, v))
	}
	return rows
}

func MakeRowExpr(t testing.TB, s string) expr.Row {
	r := MakeRow(t, s)
	var er expr.Row

	r.Iterate(func(column string, value types.Value) error {
		er.Columns = append(er.Columns, column)
		er.Exprs = append(er.Exprs, expr.LiteralValue{Value: value})
		return nil
	})

	return er
}

func MakeRowExprs(t testing.TB, s ...string) []expr.Row {
	var rows []expr.Row
	for _, v := range s {
		rows = append(rows, MakeRowExpr(t, v))
	}
	return rows
}

// MakeValue turns v into a types.Value.
func MakeValue(t testing.TB, v any) types.Value {
	t.Helper()

	vv, err := row.NewValue(v)
	assert.NoError(t, err)
	return vv
}

type Rows []row.Row

func (r Rows) RequireEqual(t testing.TB, others Rows) {
	t.Helper()

	require.Equal(t, len(r), len(others), fmt.Sprintf("expected len %d, got %d", len(r), len(others)))

	for i := range r {
		RequireRowEqual(t, r[i], others[i])
	}
}

func (r Rows) RequireEqualStream(t testing.TB, env *environment.Environment, st *stream.Stream) {
	t.Helper()

	var i int

	err := st.Iterate(env, func(env *environment.Environment) error {
		rr, ok := env.GetRow()
		require.True(t, ok)

		RequireRowEqual(t, r[i], rr)
		i++
		return nil
	})
	assert.NoError(t, err)

	require.Equal(t, len(r), i)
}

// Dump a json representation of v to os.Stdout.
func Dump(t testing.TB, v interface{}) {
	t.Helper()

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	err := enc.Encode(v)
	assert.NoError(t, err)
}

func RequireJSONEq(t testing.TB, o any, expected string) {
	t.Helper()

	data, err := json.Marshal(o)
	assert.NoError(t, err)
	require.JSONEq(t, expected, string(data))
}

// IteratorToJSONArray encodes all the objects of an iterator to a JSON array.
func IteratorToJSONArray(w io.Writer, s database.RowIterator) error {
	buf := bufio.NewWriter(w)

	buf.WriteByte('[')

	first := true
	err := s.Iterate(func(r database.Row) error {
		if !first {
			buf.WriteString(", ")
		} else {
			first = false
		}

		data, err := r.MarshalJSON()
		if err != nil {
			return err
		}

		_, err = buf.Write(data)
		return err
	})
	if err != nil {
		return err
	}

	buf.WriteByte(']')
	return buf.Flush()
}

func RequireRowEqual(t testing.TB, want, got row.Row) {
	t.Helper()

	tWant, err := json.MarshalIndent(want, "", "  ")
	require.NoError(t, err)
	tGot, err := json.MarshalIndent(got, "", "  ")
	require.NoError(t, err)

	if diff := cmp.Diff(string(tWant), string(tGot), cmp.Comparer(strings.EqualFold)); diff != "" {
		require.Failf(t, "mismatched objects, (-want, +got)", "%s", diff)
	}
}

func RequireValueEqual(t testing.TB, want, got types.Value, msg string, args ...any) {
	t.Helper()

	tWant, err := json.MarshalIndent(want, "", "  ")
	require.NoError(t, err)
	tGot, err := json.MarshalIndent(got, "", "  ")
	require.NoError(t, err)

	if diff := cmp.Diff(string(tWant), string(tGot), cmp.Comparer(strings.EqualFold)); diff != "" {
		require.Failf(t, "mismatched values, (-want, +got)", "%s\n%s", diff, fmt.Sprintf(msg, args...))
	}
}

func CloneRow(t testing.TB, r row.Row) *row.ColumnBuffer {
	t.Helper()

	var newFb row.ColumnBuffer

	err := newFb.Copy(r)
	assert.NoError(t, err)

	return &newFb
}
