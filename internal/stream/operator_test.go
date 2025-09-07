package stream_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/path"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/stream/table"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestFilter(t *testing.T) {
	tests := []struct {
		e     expr.Expr
		in    []expr.Row
		out   []row.Row
		fails bool
	}{
		{
			parser.MustParseExpr("1"),
			testutil.MakeRowExprs(t, `{"a": 1}`),
			testutil.MakeRows(t, `{"a": 1}`),
			false,
		},
		{
			parser.MustParseExpr("a > 1"),
			testutil.MakeRowExprs(t, `{"a": 1}`),
			nil,
			false,
		},
		{
			parser.MustParseExpr("a >= 1"),
			testutil.MakeRowExprs(t, `{"a": 1}`),
			testutil.MakeRows(t, `{"a": 1}`),
			false,
		},
		{
			parser.MustParseExpr("null"),
			testutil.MakeRowExprs(t, `{"a": 1}`),
			nil,
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.e.String(), func(t *testing.T) {
			s := stream.New(rows.Emit([]string{"a"}, test.in...)).Pipe(rows.Filter(test.e))
			i := 0
			err := s.Iterate(new(environment.Environment), func(r database.Row) error {
				testutil.RequireRowEqual(t, test.out[i], r)
				i++
				return nil
			})
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, rows.Filter(parser.MustParseExpr("1")).String(), "rows.Filter(1)")
	})
}

func TestTake(t *testing.T) {
	tests := []struct {
		inNumber int
		n        int
		output   int
		fails    bool
	}{
		{5, 1, 1, false},
		{5, 7, 5, false},
		{5, -1, 0, false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%d/%d", test.inNumber, test.n), func(t *testing.T) {
			var ds []expr.Row

			for i := 0; i < test.inNumber; i++ {
				ds = append(ds, testutil.MakeRowExpr(t, `{"a": `+strconv.Itoa(i)+`}`))
			}

			s := stream.New(rows.Emit([]string{"a"}, ds...))
			s = s.Pipe(rows.Take(parser.MustParseExpr(strconv.Itoa(test.n))))

			var count int
			err := s.Iterate(new(environment.Environment), func(r database.Row) error {
				count++
				return nil
			})
			if test.fails {
				require.Error(t, err)
			} else {
				if errors.Is(err, stream.ErrStreamClosed) {
					err = nil
				}
				require.NoError(t, err)
				require.Equal(t, test.output, count)
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, "rows.Take(1)", rows.Take(parser.MustParseExpr("1")).String())
	})
}

func TestSkip(t *testing.T) {
	tests := []struct {
		inNumber int
		n        int
		output   int
		fails    bool
	}{
		{5, 1, 4, false},
		{5, 7, 0, false},
		{5, -1, 5, false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%d/%d", test.inNumber, test.n), func(t *testing.T) {
			var ds []expr.Row

			for i := 0; i < test.inNumber; i++ {
				ds = append(ds, testutil.MakeRowExpr(t, `{"a": `+strconv.Itoa(i)+`}`))
			}

			s := stream.New(rows.Emit([]string{"a"}, ds...))
			s = s.Pipe(rows.Skip(parser.MustParseExpr(strconv.Itoa(test.n))))

			var count int
			err := s.Iterate(new(environment.Environment), func(r database.Row) error {
				count++
				return nil
			})
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.output, count)
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, "rows.Skip(1)", rows.Skip(parser.MustParseExpr("1")).String())
	})
}

func TestTableInsert(t *testing.T) {
	tests := []struct {
		name  string
		in    stream.Operator
		out   []row.Row
		rowid int
		fails bool
	}{
		{
			"doc with no key",
			rows.Emit([]string{"a"}, testutil.MakeRowExpr(t, `{"a": 10}`), testutil.MakeRowExpr(t, `{"a": 11}`)),
			[]row.Row{testutil.MakeRow(t, `{"a": 10}`), testutil.MakeRow(t, `{"a": 11}`)},
			1,
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, "CREATE TABLE test (a INTEGER PRIMARY KEY)")

			in := environment.New(nil, tx, nil, nil)

			s := stream.New(test.in).Pipe(table.Insert("test"))

			var i int
			err := s.Iterate(in, func(r database.Row) error {
				testutil.RequireRowEqual(t, test.out[i], r)
				i++
				return nil
			})
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, "table.Insert(\"test\")", table.Insert("test").String())
	})
}

func TestTableReplace(t *testing.T) {
	tests := []struct {
		name     string
		a, b     any
		op       stream.Operator
		expected testutil.Rows
		fails    bool
	}{
		{
			"row with key",
			1, 1,
			path.Set("b", testutil.ParseExpr(t, "2")),
			testutil.MakeRows(t, `{"a": 1, "b": 2}`),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, "CREATE TABLE test (a INTEGER PRIMARY KEY, b INTEGER)")

			testutil.MustExec(t, db, tx, "INSERT INTO test VALUES ($1, $2)", environment.Param{Value: test.a}, environment.Param{Value: test.b})

			in := environment.New(nil, tx, nil, nil)

			s := stream.New(table.Scan("test")).
				Pipe(test.op).
				Pipe(table.Replace("test"))

			var i int
			err := s.Iterate(in, func(r database.Row) error {
				got, err := row.MarshalJSON(r)
				require.NoError(t, err)
				want, err := row.MarshalJSON(test.expected[i])
				require.NoError(t, err)
				require.JSONEq(t, string(want), string(got))
				i++
				return nil
			})
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			res := testutil.MustQuery(t, db, tx, "SELECT * FROM test")
			defer res.Close()

			var got []row.Row
			err = res.Iterate(func(r database.Row) error {
				var fb row.ColumnBuffer
				err = fb.Copy(r)
				require.NoError(t, err)
				got = append(got, &fb)
				return nil
			})
			require.NoError(t, err)
			test.expected.RequireEqual(t, got)
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, table.Replace("test").String(), "table.Replace(\"test\")")
	})
}

func TestTableDelete(t *testing.T) {
	tests := []struct {
		name     string
		a        []int
		op       stream.Operator
		expected testutil.Rows
		fails    bool
	}{
		{
			"doc with key",
			[]int{1, 2, 3},
			rows.Filter(testutil.ParseExpr(t, `a > 1`)),
			testutil.MakeRows(t, `{"a": 1}`),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, "CREATE TABLE test (a INTEGER PRIMARY KEY)")

			for _, a := range test.a {
				testutil.MustExec(t, db, tx, "INSERT INTO test VALUES ($1)", environment.Param{Value: a})
			}

			env := environment.New(nil, tx, nil, nil)

			s := stream.New(table.Scan("test")).Pipe(test.op).Pipe(table.Delete("test"))

			err := s.Iterate(env, func(r database.Row) error {
				return nil
			})
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			res := testutil.MustQuery(t, db, tx, "SELECT * FROM test")
			defer res.Close()

			var got []row.Row
			err = res.Iterate(func(r database.Row) error {
				var fb row.ColumnBuffer
				err = fb.Copy(r)
				require.NoError(t, err)
				got = append(got, &fb)
				return nil
			})
			require.NoError(t, err)
			test.expected.RequireEqual(t, got)
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, table.Delete("test").String(), "table.Delete('test')")
	})
}
