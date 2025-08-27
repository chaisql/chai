package testutil

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/types"
	"github.com/stretchr/testify/require"
)

func MakeRow(t testing.TB, s string) row.Row {
	var cb row.ColumnBuffer

	err := json.Unmarshal([]byte(s), &cb)
	require.NoError(t, err)
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

	err := r.Iterate(func(column string, value types.Value) error {
		er.Columns = append(er.Columns, column)
		er.Exprs = append(er.Exprs, expr.LiteralValue{Value: value})
		return nil
	})
	require.NoError(t, err)

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
	require.NoError(t, err)
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

	it, err := st.Iterator(env)
	require.NoError(t, err)
	defer it.Close()

	for it.Next() {
		rr, err := it.Row()
		require.NoError(t, err)
		RequireRowEqual(t, r[i], rr)
		i++
	}
	require.NoError(t, it.Error())

	require.Equal(t, len(r), i)
}

// Dump a json representation of v to os.Stdout.
func Dump(t testing.TB, v any) {
	t.Helper()

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	err := enc.Encode(v)
	require.NoError(t, err)
}

func RequireJSONEq(t testing.TB, rows *sql.Rows, expected ...string) {
	t.Helper()

	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)

	vals := make([]any, len(cols))
	valPtrs := make([]any, len(cols))
	for i := range cols {
		valPtrs[i] = &vals[i]
	}

	i := 0
	for rows.Next() {
		require.Less(t, i, len(expected), "query returned too many rows")
		err = rows.Scan(valPtrs...)
		require.NoError(t, err)

		m := make(map[string]any, len(cols))
		for i := range cols {
			m[cols[i]] = vals[i]
		}

		data, err := json.Marshal(m)
		require.NoError(t, err)
		require.JSONEq(t, expected[i], string(data))
		i++
	}

	require.Equal(t, len(expected), i, "query returned too few rows")

	require.NoError(t, rows.Err())
}

func RequireJSONArrayEq(t testing.TB, rows *sql.Rows, expected string) {
	t.Helper()

	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)

	vals := make([]any, len(cols))
	valPtrs := make([]any, len(cols))
	for i := range cols {
		valPtrs[i] = &vals[i]
	}

	var arr []row.ColumnBuffer
	i := 0
	for rows.Next() {
		if i >= len(expected) {
			t.Fatalf("unexpected row %d", i)
		}
		err = rows.Scan(valPtrs...)
		require.NoError(t, err)

		var cb row.ColumnBuffer
		for i := range cols {
			v, err := row.NewValue(vals[i])
			require.NoError(t, err)
			cb.Add(cols[i], v)
		}

		arr = append(arr, cb)
		i++
	}
	require.NoError(t, rows.Err())

	data, err := json.MarshalIndent(arr, "", "  ")
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(json.RawMessage(expected), "", "  ")
	require.NoError(t, err)

	require.Equal(t, string(formatted), string(data))
}

func RequireRowEqual(t testing.TB, want, got row.Row) {
	t.Helper()

	wantCols, err := row.Columns(want)
	require.NoError(t, err)
	gotCols, err := row.Columns(got)
	require.NoError(t, err)

	slices.Sort(wantCols)
	slices.Sort(gotCols)
	require.Equal(t, wantCols, gotCols)

	for _, c := range wantCols {
		a, err := got.Get(c)
		require.NoError(t, err)
		b, err := want.Get(c)
		require.NoError(t, err)

		RequireValueEqual(t, a, b, "mismatched value for column %q", c)
	}
}

func RequireValueEqual(t testing.TB, want, got types.Value, msg string, args ...any) {
	t.Helper()

	ok, err := got.EQ(want)
	require.NoError(t, err)
	require.True(t, ok, "%s; want: %s, got: %s", fmt.Sprintf(msg, args...), want, got)
}

func CloneRow(t testing.TB, r row.Row) *row.ColumnBuffer {
	t.Helper()

	var newFb row.ColumnBuffer

	err := newFb.Copy(r)
	require.NoError(t, err)

	return &newFb
}

func SQLRowToColumnBuffer(t testing.TB, rows *sql.Rows) *row.ColumnBuffer {
	t.Helper()

	cols, err := rows.Columns()
	require.NoError(t, err)

	vals := make([]any, len(cols))
	valPtrs := make([]any, len(cols))
	for i := range cols {
		valPtrs[i] = &vals[i]
	}

	err = rows.Scan(valPtrs...)
	require.NoError(t, err)

	var cb row.ColumnBuffer

	for i, col := range cols {
		v, err := row.NewValue(vals[i])
		require.NoError(t, err)
		cb.Add(col, v)
	}

	return &cb
}
