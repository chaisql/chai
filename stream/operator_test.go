package stream_test

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/expr"
	"github.com/genjidb/genji/sql/parser"
	"github.com/genjidb/genji/stream"
	"github.com/genjidb/genji/testutil"
	"github.com/stretchr/testify/require"
)

func TestMap(t *testing.T) {
	tests := []struct {
		e     expr.Expr
		in    []document.Document
		out   []document.Document
		fails bool
	}{
		{
			parser.MustParseExpr(`{a: 10}`),
			testutil.MakeDocuments(t, `{"b": 3}`),
			testutil.MakeDocuments(t, `{"a": 10}`),
			false,
		},
		{
			parser.MustParseExpr("null"),
			testutil.MakeDocuments(t, `{"a": 10}`),
			nil,
			true,
		},
		{
			parser.MustParseExpr("{a: b}"),
			testutil.MakeDocuments(t, `{"b": 3}`),
			testutil.MakeDocuments(t, `{"a": 3}`),
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s", test.e), func(t *testing.T) {

			s := stream.New(stream.Documents(test.in...)).Pipe(stream.Map(test.e))
			i := 0
			err := s.Iterate(nil, func(out *expr.Environment) error {
				d, _ := out.GetDocument()
				require.Equal(t, test.out[i], d)
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
		require.Equal(t, stream.Map(parser.MustParseExpr("1")).String(), "map(1)")
	})
}

func TestFilter(t *testing.T) {
	tests := []struct {
		e     expr.Expr
		in    []document.Document
		out   []document.Document
		fails bool
	}{
		{
			parser.MustParseExpr("1"),
			testutil.MakeDocuments(t, `{"a": 1}`),
			testutil.MakeDocuments(t, `{"a": 1}`),
			false,
		},
		{
			parser.MustParseExpr("a > 1"),
			testutil.MakeDocuments(t, `{"a": 1}`),
			nil,
			false,
		},
		{
			parser.MustParseExpr("a >= 1"),
			testutil.MakeDocuments(t, `{"a": 1}`),
			testutil.MakeDocuments(t, `{"a": 1}`),
			false,
		},
		{
			parser.MustParseExpr("null"),
			testutil.MakeDocuments(t, `{"a": 1}`),
			nil,
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s", test.e), func(t *testing.T) {
			s := stream.New(stream.Documents(test.in...)).Pipe(stream.Filter(test.e))
			i := 0
			err := s.Iterate(nil, func(out *expr.Environment) error {
				d, _ := out.GetDocument()
				require.Equal(t, test.out[i], d)
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
		require.Equal(t, stream.Filter(parser.MustParseExpr("1")).String(), "filter(1)")
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
			var docs []document.Document

			for i := 0; i < test.inNumber; i++ {
				docs = append(docs, testutil.MakeDocument(t, `{"a": `+strconv.Itoa(i)+`}`))
			}

			s := stream.New(stream.Documents(docs...))
			s = s.Pipe(stream.Take(test.n))

			var count int
			err := s.Iterate(new(expr.Environment), func(env *expr.Environment) error {
				count++
				return nil
			})
			if test.fails {
				require.Error(t, err)
			} else {
				if err == stream.ErrStreamClosed {
					err = nil
				}
				require.NoError(t, err)
				require.Equal(t, test.output, count)
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, stream.Take(1).String(), "take(1)")
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
			var docs []document.Document

			for i := 0; i < test.inNumber; i++ {
				docs = append(docs, testutil.MakeDocument(t, `{"a": `+strconv.Itoa(i)+`}`))
			}

			s := stream.New(stream.Documents(docs...))
			s = s.Pipe(stream.Skip(test.n))

			var count int
			err := s.Iterate(new(expr.Environment), func(env *expr.Environment) error {
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
		require.Equal(t, stream.Skip(1).String(), "skip(1)")
	})
}

func TestGroupBy(t *testing.T) {
	tests := []struct {
		e     expr.Expr
		in    []document.Document
		group document.Value
		fails bool
	}{
		{
			parser.MustParseExpr("10"),
			testutil.MakeDocuments(t, `{"a": 10}`),
			document.NewIntegerValue(10),
			false,
		},
		{
			parser.MustParseExpr("null"),
			testutil.MakeDocuments(t, `{"a": 10}`),
			document.NewNullValue(),
			false,
		},
		{
			parser.MustParseExpr("a"),
			testutil.MakeDocuments(t, `{"a": 10}`),
			document.NewIntegerValue(10),
			false,
		},
		{
			parser.MustParseExpr("b"),
			testutil.MakeDocuments(t, `{"a": 10}`),
			document.NewNullValue(),
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s", test.e), func(t *testing.T) {
			var want expr.Environment
			want.Set("_group", test.group)
			want.Set("_group_expr", document.NewTextValue(fmt.Sprintf("%s", test.e)))

			s := stream.New(stream.Documents(test.in...)).Pipe(stream.GroupBy(test.e))
			err := s.Iterate(nil, func(out *expr.Environment) error {
				out.Outer = nil
				require.Equal(t, &want, out)
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
		require.Equal(t, stream.GroupBy(parser.MustParseExpr("1")).String(), "groupBy(1)")
	})
}

func generateSeqDocs(t testing.TB, max int) (docs []document.Document) {
	t.Helper()

	for i := 0; i < max; i++ {
		docs = append(docs, testutil.MakeDocument(t, `{"a": `+strconv.Itoa(i)+`}`))
	}

	return docs
}

func TestSort(t *testing.T) {
	tests := []struct {
		name     string
		sortExpr expr.Expr
		values   []document.Document
		want     []document.Document
		fails    bool
		desc     bool
	}{
		{
			"ASC",
			parser.MustParseExpr("a"),
			[]document.Document{
				testutil.MakeDocument(t, `{"a": 0}`),
				testutil.MakeDocument(t, `{"a": null}`),
				testutil.MakeDocument(t, `{"a": true}`),
			},
			[]document.Document{
				testutil.MakeDocument(t, `{"a": null}`),
				testutil.MakeDocument(t, `{"a": true}`),
				testutil.MakeDocument(t, `{"a": 0}`),
			},
			false,
			false,
		},
		{
			"DESC",
			parser.MustParseExpr("a"),
			[]document.Document{
				testutil.MakeDocument(t, `{"a": 0}`),
				testutil.MakeDocument(t, `{"a": null}`),
				testutil.MakeDocument(t, `{"a": true}`),
			},
			[]document.Document{
				testutil.MakeDocument(t, `{"a": 0}`),
				testutil.MakeDocument(t, `{"a": true}`),
				testutil.MakeDocument(t, `{"a": null}`),
			},
			false,
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := stream.New(stream.Documents(test.values...))
			if test.desc {
				s = s.Pipe(stream.SortReverse(test.sortExpr))
			} else {
				s = s.Pipe(stream.Sort(test.sortExpr))
			}

			var got []document.Document
			err := s.Iterate(new(expr.Environment), func(env *expr.Environment) error {
				d, ok := env.GetDocument()
				require.True(t, ok)
				got = append(got, d)
				return nil
			})
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.want, got)
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, `sort(a)`, stream.Sort(parser.MustParseExpr("a")).String())
	})
}

func TestTableInsert(t *testing.T) {
	tests := []struct {
		name  string
		in    stream.Operator
		out   []document.Document
		docid int
		fails bool
	}{
		{
			"doc with no key",
			stream.Documents(testutil.MakeDocument(t, `{"a": 10}`), testutil.MakeDocument(t, `{"a": 11}`)),
			[]document.Document{testutil.MakeDocument(t, `{"a": 10}`), testutil.MakeDocument(t, `{"a": 11}`)},
			1,
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec("CREATE TABLE test (a INTEGER)")
			require.NoError(t, err)

			tx, err := db.Begin(true)
			require.NoError(t, err)
			defer tx.Rollback()

			in := expr.NewEnvironment(nil)
			in.Tx = tx.Transaction

			s := stream.New(test.in).Pipe(stream.TableInsert("test"))

			var i int
			err = s.Iterate(in, func(out *expr.Environment) error {
				d, ok := out.GetDocument()
				require.True(t, ok)

				testutil.RequireDocEqual(t, test.out[i], d)
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
		require.Equal(t, stream.TableInsert("test").String(), "tableInsert('test')")
	})
}

func TestTableReplace(t *testing.T) {
	tests := []struct {
		name                      string
		docsInTable, in, expected testutil.Docs
		fails                     bool
	}{
		{
			"doc with key",
			testutil.MakeDocuments(t, `{"a": 1, "b": 1}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`),
			testutil.MakeDocuments(t, `{"a": 1, "b": 2}`),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec("CREATE TABLE test (a INTEGER PRIMARY KEY, b INTEGER)")
			require.NoError(t, err)

			tx, err := db.Begin(true)
			require.NoError(t, err)
			defer tx.Rollback()

			tb, err := tx.Catalog.GetTable(tx.Transaction, "test")
			require.NoError(t, err)

			for i, doc := range test.docsInTable {
				err = tx.Exec("INSERT INTO test VALUES ?", doc)
				require.NoError(t, err)
				kk, err := doc.GetByField("a")
				require.NoError(t, err)
				k, err := tb.EncodeValue(kk)
				require.NoError(t, err)
				test.in[i].(*document.FieldBuffer).EncodedKey = k
			}

			var in expr.Environment
			in.Tx = tx.Transaction

			s := stream.New(stream.Documents(test.in...)).Pipe(stream.TableReplace("test"))

			var i int
			err = s.Iterate(&in, func(out *expr.Environment) error {
				d, ok := out.GetDocument()
				require.True(t, ok)

				got, err := json.Marshal(d)
				require.NoError(t, err)
				want, err := json.Marshal(test.expected[i])
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

			res, err := tx.Query("SELECT * FROM test")
			require.NoError(t, err)
			defer res.Close()

			var got []document.Document
			err = res.Iterate(func(d document.Document) error {
				var fb document.FieldBuffer
				fb.Copy(d)
				got = append(got, fb)
				return nil
			})
			require.NoError(t, err)
			test.expected.RequireEqual(t, got)
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, stream.TableReplace("test").String(), "tableReplace('test')")
	})
}

func TestTableDelete(t *testing.T) {
	tests := []struct {
		name                  string
		docsInTable, expected testutil.Docs
		in                    document.Document
		fails                 bool
	}{
		{
			"doc with key",
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 2}`, `{"a": 3}`),
			testutil.MakeDocuments(t, `{"a": 1}`, `{"a": 3}`),
			testutil.MakeDocument(t, `{"a": 2}`),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec("CREATE TABLE test (a INTEGER PRIMARY KEY)")
			require.NoError(t, err)

			for _, doc := range test.docsInTable {
				err = db.Exec("INSERT INTO test VALUES ?", doc)
				require.NoError(t, err)
			}

			tx, err := db.Begin(true)
			require.NoError(t, err)
			defer tx.Rollback()

			var env expr.Environment
			env.Tx = tx.Transaction

			tb, err := tx.Catalog.GetTable(tx.Transaction, "test")
			require.NoError(t, err)
			kk, err := test.in.GetByField("a")
			require.NoError(t, err)

			k, err := tb.EncodeValue(kk)
			require.NoError(t, err)
			test.in.(*document.FieldBuffer).EncodedKey = k

			s := stream.New(stream.Documents(test.in)).Pipe(stream.TableDelete("test"))

			err = s.Iterate(&env, func(out *expr.Environment) error {
				d, _ := out.GetDocument()
				require.Equal(t, test.in, d)
				return nil
			})
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			res, err := tx.Query("SELECT * FROM test")
			require.NoError(t, err)
			defer res.Close()

			var got []document.Document
			err = res.Iterate(func(d document.Document) error {
				var fb document.FieldBuffer
				fb.Copy(d)
				got = append(got, fb)
				return nil
			})
			require.NoError(t, err)
			test.expected.RequireEqual(t, got)
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, stream.TableDelete("test").String(), "tableDelete('test')")
	})
}

func TestDistinct(t *testing.T) {
	tests := []struct {
		name   string
		values testutil.Docs
		want   testutil.Docs
		fails  bool
	}{
		{
			"all different",
			testutil.MakeDocuments(t, `{"a": 0}`, `{"a": null}`, `{"a": true}`),
			testutil.MakeDocuments(t, `{"a": 0}`, `{"a": null}`, `{"a": true}`),
			false,
		},
		{
			"some duplicates",
			testutil.MakeDocuments(t, `{"a": 0}`, `{"a": 0}`, `{"a": null}`, `{"a": null}`, `{"a": true}`, `{"a": true}`, `{"a": [1, 2]}`, `{"a": [1, 2]}`),
			testutil.MakeDocuments(t, `{"a": 0}`, `{"a": null}`, `{"a": true}`, `{"a": [1, 2]}`),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := stream.New(stream.Documents(test.values...))
			s = s.Pipe(stream.Distinct())

			var got []document.Document
			err := s.Iterate(new(expr.Environment), func(env *expr.Environment) error {
				d, ok := env.GetDocument()
				require.True(t, ok)
				var fb document.FieldBuffer
				err := fb.Copy(d)
				require.NoError(t, err)
				got = append(got, &fb)
				return nil
			})
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				test.want.RequireEqual(t, got)
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, `distinct()`, stream.Distinct().String())
	})
}

func TestSet(t *testing.T) {
	tests := []struct {
		path    string
		e       expr.Expr
		in, out []document.Document
		fails   bool
	}{
		{
			"a[0].b",
			parser.MustParseExpr(`10`),
			testutil.MakeDocuments(t, `{"a": [{}]}`),
			testutil.MakeDocuments(t, `{"a": [{"b": 10}]}`),
			false,
		},
		{
			"a[2]",
			parser.MustParseExpr(`10`),
			testutil.MakeDocuments(t, `{"a": [1]}`, `{"a": [1, 2, 3]}`),
			testutil.MakeDocuments(t, `{"a": [1, 2, 10]}`),
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s", test.e), func(t *testing.T) {
			p, err := parser.ParsePath(test.path)
			require.NoError(t, err)
			s := stream.New(stream.Documents(test.in...)).Pipe(stream.Set(p, test.e))
			i := 0
			err = s.Iterate(nil, func(out *expr.Environment) error {
				d, _ := out.GetDocument()
				require.Equal(t, test.out[i], d)
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
		require.Equal(t, stream.Set(document.NewPath("a", "b"), parser.MustParseExpr("1")).String(), "set(a.b, 1)")
	})
}

func TestUnset(t *testing.T) {
	tests := []struct {
		path    string
		in, out []document.Document
		fails   bool
	}{
		{
			"a",
			testutil.MakeDocuments(t, `{"a": 10, "b": 20}`),
			testutil.MakeDocuments(t, `{"b": 20}`),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			s := stream.New(stream.Documents(test.in...)).Pipe(stream.Unset(test.path))
			i := 0
			err := s.Iterate(nil, func(out *expr.Environment) error {
				d, _ := out.GetDocument()
				require.Equal(t, test.out[i], d)
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
		require.Equal(t, stream.Unset("a").String(), "unset(a)")
	})
}

func TestIterRename(t *testing.T) {
	tests := []struct {
		fieldNames []string
		in, out    []document.Document
		fails      bool
	}{
		{
			[]string{"c", "d"},
			testutil.MakeDocuments(t, `{"a": 10, "b": 20}`),
			testutil.MakeDocuments(t, `{"c": 10, "d": 20}`),
			false,
		},
		{
			[]string{"c", "d", "e"},
			testutil.MakeDocuments(t, `{"a": 10, "b": 20}`),
			nil,
			true,
		},
		{
			[]string{"c"},
			testutil.MakeDocuments(t, `{"a": 10, "b": 20}`),
			nil,
			true,
		},
	}

	for _, test := range tests {
		s := stream.New(stream.Documents(test.in...)).Pipe(stream.IterRename(test.fieldNames...))
		t.Run(s.String(), func(t *testing.T) {
			i := 0
			err := s.Iterate(nil, func(out *expr.Environment) error {
				d, _ := out.GetDocument()
				require.Equal(t, test.out[i], d)
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
		require.Equal(t, stream.IterRename("a", "b", "c").String(), "iterRename(a, b, c)")
	})
}
