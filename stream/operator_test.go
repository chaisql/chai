package stream_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/parser"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/stream"
	"github.com/genjidb/genji/testutil"
	"github.com/stretchr/testify/require"
)

func docFromJSON(d string) *document.FieldBuffer {
	var fb document.FieldBuffer

	err := fb.UnmarshalJSON([]byte(d))
	if err != nil {
		panic(err)
	}
	return &fb
}

func TestMap(t *testing.T) {
	tests := []struct {
		e       expr.Expr
		in, out *expr.Environment
		fails   bool
	}{
		{
			parser.MustParseExpr(`{a: 10}`),
			expr.NewEnvironment(docFromJSON(`{"b": 3}`)),
			expr.NewEnvironment(docFromJSON(`{"a": 10}`)),
			false,
		},
		{
			parser.MustParseExpr("null"),
			expr.NewEnvironment(docFromJSON(`{"a": 10}`)),
			nil,
			true,
		},
		{
			parser.MustParseExpr("{a: b}"),
			expr.NewEnvironment(docFromJSON(`{"b": 3}`)),
			expr.NewEnvironment(docFromJSON(`{"a": 3}`)),
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s", test.e), func(t *testing.T) {
			if test.out != nil {
				test.out.Outer = test.in
			}

			err := stream.Map(test.e).Iterate(test.in, func(out *expr.Environment) error {
				require.Equal(t, test.out, out)
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
		e       expr.Expr
		in, out *expr.Environment
		fails   bool
	}{
		{
			parser.MustParseExpr("1"),
			expr.NewEnvironment(docFromJSON(`{"a": 1}`)),
			expr.NewEnvironment(docFromJSON(`{"a": 1}`)),
			false,
		},
		{
			parser.MustParseExpr("a > 1"),
			expr.NewEnvironment(docFromJSON(`{"a": 1}`)),
			nil,
			false,
		},
		{
			parser.MustParseExpr("a >= 1"),
			expr.NewEnvironment(docFromJSON(`{"a": 1}`)),
			expr.NewEnvironment(docFromJSON(`{"a": 1}`)),
			false,
		},
		{
			parser.MustParseExpr("null"),
			expr.NewEnvironment(docFromJSON(`{"a": 1}`)),
			nil,
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s", test.e), func(t *testing.T) {
			err := stream.Filter(test.e).Iterate(test.in, func(out *expr.Environment) error {
				require.Equal(t, test.out, out)
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
		{5, -1, 1, false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%d/%d", test.inNumber, test.n), func(t *testing.T) {
			var docs []document.Document

			for i := 0; i < test.inNumber; i++ {
				docs = append(docs, docFromJSON(`{"a": `+strconv.Itoa(i)+`}`))
			}

			s := stream.New(stream.Documents(docs...))
			s = s.Pipe(stream.Take(test.n))

			var count int
			err := s.Op.Iterate(new(expr.Environment), func(env *expr.Environment) error {
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
				docs = append(docs, docFromJSON(`{"a": `+strconv.Itoa(i)+`}`))
			}

			s := stream.New(stream.Documents(docs...))
			s = s.Pipe(stream.Skip(test.n))

			var count int
			err := s.Op.Iterate(new(expr.Environment), func(env *expr.Environment) error {
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
		in    *expr.Environment
		group document.Value
		fails bool
	}{
		{
			parser.MustParseExpr("10"),
			expr.NewEnvironment(docFromJSON(`{"a": 10}`)),
			document.NewIntegerValue(10),
			false,
		},
		{
			parser.MustParseExpr("null"),
			expr.NewEnvironment(docFromJSON(`{"a": 10}`)),
			document.NewNullValue(),
			false,
		},
		{
			parser.MustParseExpr("a"),
			expr.NewEnvironment(docFromJSON(`{"a": 10}`)),
			document.NewIntegerValue(10),
			false,
		},
		{
			parser.MustParseExpr("b"),
			expr.NewEnvironment(docFromJSON(`{"a": 10}`)),
			document.NewNullValue(),
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s", test.e), func(t *testing.T) {
			var want expr.Environment
			want.Outer = test.in
			want.Set("_group", test.group)
			want.Set("_group_expr", document.NewTextValue(fmt.Sprintf("%s", test.e)))

			err := stream.GroupBy(test.e).Iterate(test.in, func(out *expr.Environment) error {
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

func generateSeqDocs(max int) (docs []document.Document) {
	for i := 0; i < max; i++ {
		docs = append(docs, docFromJSON(`{"a": `+strconv.Itoa(i)+`}`))
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
				docFromJSON(`{"a": 0}`),
				docFromJSON(`{"a": null}`),
				docFromJSON(`{"a": true}`),
			},
			[]document.Document{
				docFromJSON(`{"a": null}`),
				docFromJSON(`{"a": true}`),
				docFromJSON(`{"a": 0}`),
			},
			false,
			false,
		},
		{
			"DESC",
			parser.MustParseExpr("a"),
			[]document.Document{
				docFromJSON(`{"a": 0}`),
				docFromJSON(`{"a": null}`),
				docFromJSON(`{"a": true}`),
			},
			[]document.Document{
				docFromJSON(`{"a": 0}`),
				docFromJSON(`{"a": true}`),
				docFromJSON(`{"a": null}`),
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
			err := s.Op.Iterate(new(expr.Environment), func(env *expr.Environment) error {
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
		name    string
		in, out *expr.Environment
		docid   int
		fails   bool
	}{
		{
			"doc with no key",
			expr.NewEnvironment(docFromJSON(`{"a": 10}`)),
			expr.NewEnvironment(docFromJSON(`{"a": 10}`)),
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

			test.in.Tx = tx.Transaction

			if test.out != nil {
				test.out.Outer = test.in
				tb, err := tx.GetTable("test")
				require.NoError(t, err)
				k, err := tb.EncodeValue(document.NewIntegerValue(1))
				require.NoError(t, err)
				test.out.Doc.(*document.FieldBuffer).EncodedKey = k
			}

			ti := stream.TableInsert("test")

			err = ti.Iterate(test.in, func(out *expr.Environment) error {
				require.Equal(t, test.out, out)
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
		name                  string
		docsInTable, expected testutil.Docs
		in                    *expr.Environment
		fails                 bool
	}{
		{
			"doc with key",
			testutil.MakeDocuments(`{"a": 1, "b": 1}`),
			testutil.MakeDocuments(`{"a": 1, "b": 2}`),
			expr.NewEnvironment(docFromJSON(`{"a": 1, "b": 2}`)),
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

			for _, doc := range test.docsInTable {
				err = db.Exec("INSERT INTO test VALUES ?", doc)
				require.NoError(t, err)
			}

			tx, err := db.Begin(true)
			require.NoError(t, err)
			defer tx.Rollback()

			test.in.Tx = tx.Transaction
			tb, err := tx.GetTable("test")
			require.NoError(t, err)
			kk, err := test.in.Doc.GetByField("a")
			require.NoError(t, err)

			k, err := tb.EncodeValue(kk)
			require.NoError(t, err)
			test.in.Doc.(*document.FieldBuffer).EncodedKey = k

			ti := stream.TableReplace("test")

			err = ti.Iterate(test.in, func(out *expr.Environment) error {
				require.Equal(t, test.in, out)
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
		in                    *expr.Environment
		fails                 bool
	}{
		{
			"doc with key",
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 2}`, `{"a": 3}`),
			testutil.MakeDocuments(`{"a": 1}`, `{"a": 3}`),
			expr.NewEnvironment(docFromJSON(`{"a": 2}`)),
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

			test.in.Tx = tx.Transaction

			tb, err := tx.GetTable("test")
			require.NoError(t, err)
			kk, err := test.in.Doc.GetByField("a")
			require.NoError(t, err)

			k, err := tb.EncodeValue(kk)
			require.NoError(t, err)
			test.in.Doc.(*document.FieldBuffer).EncodedKey = k

			ti := stream.TableDelete("test")

			err = ti.Iterate(test.in, func(out *expr.Environment) error {
				require.Equal(t, test.in, out)
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
			testutil.MakeDocuments(`{"a": 0}`, `{"a": null}`, `{"a": true}`),
			testutil.MakeDocuments(`{"a": 0}`, `{"a": null}`, `{"a": true}`),
			false,
		},
		{
			"some duplicates",
			testutil.MakeDocuments(`{"a": 0}`, `{"a": 0}`, `{"a": null}`, `{"a": null}`, `{"a": true}`, `{"a": true}`, `{"a": [1, 2]}`, `{"a": [1, 2]}`),
			testutil.MakeDocuments(`{"a": 0}`, `{"a": null}`, `{"a": true}`, `{"a": [1, 2]}`),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := stream.New(stream.Documents(test.values...))
			s = s.Pipe(stream.Distinct())

			var got []document.Document
			err := s.Op.Iterate(new(expr.Environment), func(env *expr.Environment) error {
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
		in, out *expr.Environment
		fails   bool
	}{
		{
			"a[0].b",
			parser.MustParseExpr(`10`),
			expr.NewEnvironment(docFromJSON(`{"a": [{}]}`)),
			expr.NewEnvironment(docFromJSON(`{"a": [{"b": 10}]}`)),
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s", test.e), func(t *testing.T) {
			if test.out != nil {
				test.out.Outer = test.in
			}

			p, err := parser.ParsePath(test.path)
			require.NoError(t, err)
			err = stream.Set(p, test.e).Iterate(test.in, func(out *expr.Environment) error {
				require.Equal(t, test.out, out)
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
		in, out *expr.Environment
		fails   bool
	}{
		{
			"a",
			expr.NewEnvironment(docFromJSON(`{"a": 10, "b": 20}`)),
			expr.NewEnvironment(docFromJSON(`{"b": 20}`)),
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s", test.path), func(t *testing.T) {
			if test.out != nil {
				test.out.Outer = test.in
			}

			err := stream.Unset(test.path).Iterate(test.in, func(out *expr.Environment) error {
				require.Equal(t, test.out, out)
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
		require.Equal(t, stream.Set(document.NewPath("a", "b"), parser.MustParseExpr("1")).String(), "unset(a)")
	})
}
