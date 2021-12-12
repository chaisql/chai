package stream_test

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestFilter(t *testing.T) {
	tests := []struct {
		e     expr.Expr
		in    []expr.Expr
		out   []types.Document
		fails bool
	}{
		{
			parser.MustParseExpr("1"),
			testutil.ParseExprs(t, `{"a": 1}`),
			testutil.MakeDocuments(t, `{"a": 1}`),
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
			testutil.MakeDocuments(t, `{"a": 1}`),
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
			s := stream.New(stream.DocsEmit(test.in...)).Pipe(stream.DocsFilter(test.e))
			i := 0
			err := s.Iterate(new(environment.Environment), func(out *environment.Environment) error {
				d, _ := out.GetDocument()
				require.Equal(t, test.out[i], d)
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
		require.Equal(t, stream.DocsFilter(parser.MustParseExpr("1")).String(), "docs.Filter(1)")
	})
}

func TestTake(t *testing.T) {
	tests := []struct {
		inNumber int
		n        int64
		output   int
		fails    bool
	}{
		{5, 1, 1, false},
		{5, 7, 5, false},
		{5, -1, 0, false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%d/%d", test.inNumber, test.n), func(t *testing.T) {
			var docs []expr.Expr

			for i := 0; i < test.inNumber; i++ {
				docs = append(docs, testutil.ParseExpr(t, `{"a": `+strconv.Itoa(i)+`}`))
			}

			s := stream.New(stream.DocsEmit(docs...))
			s = s.Pipe(stream.DocsTake(test.n))

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
		require.Equal(t, stream.DocsTake(1).String(), "docs.Take(1)")
	})
}

func TestSkip(t *testing.T) {
	tests := []struct {
		inNumber int
		n        int64
		output   int
		fails    bool
	}{
		{5, 1, 4, false},
		{5, 7, 0, false},
		{5, -1, 5, false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%d/%d", test.inNumber, test.n), func(t *testing.T) {
			var docs []expr.Expr

			for i := 0; i < test.inNumber; i++ {
				docs = append(docs, testutil.ParseExpr(t, `{"a": `+strconv.Itoa(i)+`}`))
			}

			s := stream.New(stream.DocsEmit(docs...))
			s = s.Pipe(stream.DocsSkip(test.n))

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
		require.Equal(t, stream.DocsSkip(1).String(), "docs.Skip(1)")
	})
}

func generateSeqDocs(t testing.TB, max int) (docs []types.Document) {
	t.Helper()

	for i := 0; i < max; i++ {
		docs = append(docs, testutil.MakeDocument(t, `{"a": `+strconv.Itoa(i)+`}`))
	}

	return docs
}

func TestTableInsert(t *testing.T) {
	tests := []struct {
		name  string
		in    stream.Operator
		out   []types.Document
		docid int
		fails bool
	}{
		{
			"doc with no key",
			stream.DocsEmit(testutil.ParseExpr(t, `{"a": 10}`), testutil.ParseExpr(t, `{"a": 11}`)),
			[]types.Document{testutil.MakeDocument(t, `{"a": 10}`), testutil.MakeDocument(t, `{"a": 11}`)},
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
			in.Catalog = db.Catalog

			s := stream.New(test.in).Pipe(stream.TableInsert("test"))

			var i int
			err := s.Iterate(in, func(out *environment.Environment) error {
				d, ok := out.GetDocument()
				require.True(t, ok)

				testutil.RequireDocEqual(t, test.out[i], d)
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
		require.Equal(t, "table.Insert(\"test\")", stream.TableInsert("test").String())
	})
}

func TestTableReplace(t *testing.T) {
	tests := []struct {
		name        string
		docsInTable testutil.Docs
		op          stream.Operator
		expected    testutil.Docs
		fails       bool
	}{
		{
			"doc with key",
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`),
			stream.PathsSet(testutil.ParseDocumentPath(t, "b"), testutil.ParseExpr(t, "2")),
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`),
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
			in.Catalog = db.Catalog

			s := stream.New(stream.TableScan("test")).
				Pipe(test.op).
				Pipe(stream.TableReplace("test"))

			var i int
			err := s.Iterate(&in, func(out *environment.Environment) error {
				d, ok := out.GetDocument()
				require.True(t, ok)

				got, err := json.Marshal(d)
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

			var got []types.Document
			err = res.Iterate(func(d types.Document) error {
				var fb document.FieldBuffer
				fb.Copy(d)
				got = append(got, fb)
				return nil
			})
			assert.NoError(t, err)
			test.expected.RequireEqual(t, got)
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, stream.TableReplace("test").String(), "table.Replace(\"test\")")
	})
}

func TestTableDelete(t *testing.T) {
	tests := []struct {
		name        string
		docsInTable testutil.Docs
		op          stream.Operator
		expected    testutil.Docs
		fails       bool
	}{
		{
			"doc with key",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`, `{"a": 3}`),
			stream.DocsFilter(testutil.ParseExpr(t, `a > 1`)),
			testutil.MakeDocuments(t, `{"a": 1}`),
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
			env.Catalog = db.Catalog

			s := stream.New(stream.TableScan("test")).Pipe(test.op).Pipe(stream.TableDelete("test"))

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

			var got []types.Document
			err = res.Iterate(func(d types.Document) error {
				var fb document.FieldBuffer
				fb.Copy(d)
				got = append(got, fb)
				return nil
			})
			assert.NoError(t, err)
			test.expected.RequireEqual(t, got)
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, stream.TableDelete("test").String(), "table.Delete('test')")
	})
}

func TestPathsRename(t *testing.T) {
	tests := []struct {
		fieldNames []string
		in         []expr.Expr
		out        []types.Document
		fails      bool
	}{
		{
			[]string{"c", "d"},
			testutil.ParseExprs(t, `{"a": 10, "b": 20}`),
			testutil.MakeDocuments(t, `{"c": 10, "d": 20}`),
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
		s := stream.New(stream.DocsEmit(test.in...)).Pipe(stream.PathsRename(test.fieldNames...))
		t.Run(s.String(), func(t *testing.T) {
			i := 0
			err := s.Iterate(new(environment.Environment), func(out *environment.Environment) error {
				d, _ := out.GetDocument()
				require.Equal(t, test.out[i], d)
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
		require.Equal(t, stream.PathsRename("a", "b", "c").String(), "paths.Rename(a, b, c)")
	})
}
