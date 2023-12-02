package stream_test

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/object"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/path"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/stream/table"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/chaisql/chai/internal/types"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestFilter(t *testing.T) {
	tests := []struct {
		e     expr.Expr
		in    []expr.Expr
		out   []types.Object
		fails bool
	}{
		{
			parser.MustParseExpr("1"),
			testutil.ParseExprs(t, `{"a": 1}`),
			testutil.MakeObjects(t, `{"a": 1}`),
			false,
		},
		{
			parser.MustParseExpr("a > 1"),
			testutil.ParseExprs(t, `{"a": 1}`),
			nil,
			false,
		},
		{
			parser.MustParseExpr("a >= 1"),
			testutil.ParseExprs(t, `{"a": 1}`),
			testutil.MakeObjects(t, `{"a": 1}`),
			false,
		},
		{
			parser.MustParseExpr("null"),
			testutil.ParseExprs(t, `{"a": 1}`),
			nil,
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.e.String(), func(t *testing.T) {
			s := stream.New(rows.Emit(test.in...)).Pipe(rows.Filter(test.e))
			i := 0
			err := s.Iterate(new(environment.Environment), func(out *environment.Environment) error {
				r, _ := out.GetRow()
				require.Equal(t, test.out[i], r.Object())
				i++
				return nil
			})
			if test.fails {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

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
			var ds []expr.Expr

			for i := 0; i < test.inNumber; i++ {
				ds = append(ds, testutil.ParseExpr(t, `{"a": `+strconv.Itoa(i)+`}`))
			}

			s := stream.New(rows.Emit(ds...))
			s = s.Pipe(rows.Take(parser.MustParseExpr(strconv.Itoa(test.n))))

			var count int
			err := s.Iterate(new(environment.Environment), func(env *environment.Environment) error {
				count++
				return nil
			})
			if test.fails {
				assert.Error(t, err)
			} else {
				if errors.Is(err, stream.ErrStreamClosed) {
					err = nil
				}
				assert.NoError(t, err)
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
			var ds []expr.Expr

			for i := 0; i < test.inNumber; i++ {
				ds = append(ds, testutil.ParseExpr(t, `{"a": `+strconv.Itoa(i)+`}`))
			}

			s := stream.New(rows.Emit(ds...))
			s = s.Pipe(rows.Skip(parser.MustParseExpr(strconv.Itoa(test.n))))

			var count int
			err := s.Iterate(new(environment.Environment), func(env *environment.Environment) error {
				count++
				return nil
			})
			if test.fails {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
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
		out   []types.Object
		rowid int
		fails bool
	}{
		{
			"doc with no key",
			rows.Emit(testutil.ParseExpr(t, `{"a": 10}`), testutil.ParseExpr(t, `{"a": 11}`)),
			[]types.Object{testutil.MakeObject(t, `{"a": 10}`), testutil.MakeObject(t, `{"a": 11}`)},
			1,
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, "CREATE TABLE test (a INTEGER)")

			in := &environment.Environment{}
			in.Tx = tx

			s := stream.New(test.in).Pipe(table.Insert("test"))

			var i int
			err := s.Iterate(in, func(out *environment.Environment) error {
				r, ok := out.GetRow()
				require.True(t, ok)

				testutil.RequireObjEqual(t, test.out[i], r.Object())
				i++
				return nil
			})
			if test.fails {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, "table.Insert(\"test\")", table.Insert("test").String())
	})
}

func TestTableReplace(t *testing.T) {
	tests := []struct {
		name        string
		docsInTable testutil.Objs
		op          stream.Operator
		expected    testutil.Objs
		fails       bool
	}{
		{
			"doc with key",
			testutil.MakeObjects(t, `{"a": 1, "b": 1}`),
			path.Set(testutil.ParseObjectPath(t, "b"), testutil.ParseExpr(t, "2")),
			testutil.MakeObjects(t, `{"a": 1, "b": 2}`),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, "CREATE TABLE test (a INTEGER PRIMARY KEY, b INTEGER)")

			for _, doc := range test.docsInTable {
				testutil.MustExec(t, db, tx, "INSERT INTO test VALUES ?", environment.Param{Value: doc})
			}

			in := environment.Environment{}
			in.Tx = tx

			s := stream.New(table.Scan("test")).
				Pipe(test.op).
				Pipe(table.Replace("test"))

			var i int
			err := s.Iterate(&in, func(out *environment.Environment) error {
				r, ok := out.GetRow()
				require.True(t, ok)

				got, err := json.Marshal(r)
				assert.NoError(t, err)
				want, err := json.Marshal(test.expected[i])
				assert.NoError(t, err)
				require.JSONEq(t, string(want), string(got))
				i++
				return nil
			})
			if test.fails {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			res := testutil.MustQuery(t, db, tx, "SELECT * FROM test")
			defer res.Close()

			var got []types.Object
			err = res.Iterate(func(row database.Row) error {
				var fb object.FieldBuffer
				fb.Copy(row.Object())
				got = append(got, &fb)
				return nil
			})
			assert.NoError(t, err)
			test.expected.RequireEqual(t, got)
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, table.Replace("test").String(), "table.Replace(\"test\")")
	})
}

func TestTableDelete(t *testing.T) {
	tests := []struct {
		name        string
		docsInTable testutil.Objs
		op          stream.Operator
		expected    testutil.Objs
		fails       bool
	}{
		{
			"doc with key",
			testutil.MakeObjects(t, `{"a": 1}`, `{"a": 2}`, `{"a": 3}`),
			rows.Filter(testutil.ParseExpr(t, `a > 1`)),
			testutil.MakeObjects(t, `{"a": 1}`),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, "CREATE TABLE test (a INTEGER PRIMARY KEY)")

			for _, doc := range test.docsInTable {
				testutil.MustExec(t, db, tx, "INSERT INTO test VALUES ?", environment.Param{Value: doc})
			}

			var env environment.Environment
			env.Tx = tx

			s := stream.New(table.Scan("test")).Pipe(test.op).Pipe(table.Delete("test"))

			err := s.Iterate(&env, func(out *environment.Environment) error {
				return nil
			})
			if test.fails {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			res := testutil.MustQuery(t, db, tx, "SELECT * FROM test")
			defer res.Close()

			var got []types.Object
			err = res.Iterate(func(row database.Row) error {
				var fb object.FieldBuffer
				fb.Copy(row.Object())
				got = append(got, &fb)
				return nil
			})
			assert.NoError(t, err)
			test.expected.RequireEqual(t, got)
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, table.Delete("test").String(), "table.Delete('test')")
	})
}

func TestPathsRename(t *testing.T) {
	tests := []struct {
		fieldNames []string
		in         []expr.Expr
		out        []types.Object
		fails      bool
	}{
		{
			[]string{"c", "d"},
			testutil.ParseExprs(t, `{"a": 10, "b": 20}`),
			testutil.MakeObjects(t, `{"c": 10, "d": 20}`),
			false,
		},
		{
			[]string{"c", "d", "e"},
			testutil.ParseExprs(t, `{"a": 10, "b": 20}`),
			nil,
			true,
		},
		{
			[]string{"c"},
			testutil.ParseExprs(t, `{"a": 10, "b": 20}`),
			nil,
			true,
		},
	}

	for _, test := range tests {
		s := stream.New(rows.Emit(test.in...)).Pipe(path.PathsRename(test.fieldNames...))
		t.Run(s.String(), func(t *testing.T) {
			i := 0
			err := s.Iterate(new(environment.Environment), func(out *environment.Environment) error {
				r, _ := out.GetRow()
				require.Equal(t, test.out[i], r.Object())
				i++
				return nil
			})
			if test.fails {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, path.PathsRename("a", "b", "c").String(), "paths.Rename(a, b, c)")
	})
}
