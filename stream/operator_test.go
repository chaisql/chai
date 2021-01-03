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

			op, err := stream.Map(test.e).Op()
			require.NoError(t, err)
			env, err := op(test.in)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.out, env)
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
			op, err := stream.Filter(test.e).Op()
			require.NoError(t, err)
			env, err := op(test.in)

			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.out, env)
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
		n        expr.Expr
		output   int
		fails    bool
	}{
		{5, parser.MustParseExpr("1"), 1, false},
		{5, parser.MustParseExpr("7"), 5, false},
		{5, parser.MustParseExpr("1.1"), 1, false},
		{5, parser.MustParseExpr("true"), 1, false},
		{5, parser.MustParseExpr("1 + 1"), 2, false},
		{5, parser.MustParseExpr("a"), 1, true},
		{5, parser.MustParseExpr("'hello'"), 1, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%d/%s", test.inNumber, test.n), func(t *testing.T) {
			var docs []document.Document

			for i := 0; i < test.inNumber; i++ {
				docs = append(docs, docFromJSON(`{"a": `+strconv.Itoa(i)+`}`))
			}

			s := stream.New(stream.NewDocumentIterator(docs...))
			s = s.Pipe(stream.Take(test.n))

			var count int
			err := s.Iterate(func(env *expr.Environment) error {
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
		require.Equal(t, stream.Take(parser.MustParseExpr("1")).String(), "take(1)")
	})
}

func TestSkip(t *testing.T) {
	tests := []struct {
		inNumber int
		n        expr.Expr
		output   int
		fails    bool
	}{
		{5, parser.MustParseExpr("1"), 4, false},
		{5, parser.MustParseExpr("7"), 0, false},
		{5, parser.MustParseExpr("1.1"), 4, false},
		{5, parser.MustParseExpr("true"), 4, false},
		{5, parser.MustParseExpr("1 + 1"), 3, false},
		{5, parser.MustParseExpr("a"), 1, true},
		{5, parser.MustParseExpr("'hello'"), 1, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%d/%s", test.inNumber, test.n), func(t *testing.T) {
			var docs []document.Document

			for i := 0; i < test.inNumber; i++ {
				docs = append(docs, docFromJSON(`{"a": `+strconv.Itoa(i)+`}`))
			}

			s := stream.New(stream.NewDocumentIterator(docs...))
			s = s.Pipe(stream.Skip(test.n))

			var count int
			err := s.Iterate(func(env *expr.Environment) error {
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
		require.Equal(t, stream.Skip(parser.MustParseExpr("1")).String(), "skip(1)")
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

			op, err := stream.GroupBy(test.e).Op()
			require.NoError(t, err)
			env, err := op(test.in)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, &want, env)
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

func TestReduce(t *testing.T) {
	tests := []struct {
		name      string
		groupBy   expr.Expr
		seed, acc expr.Expr
		in        []document.Document
		want      []document.Document
		fails     bool
	}{
		{
			"count",
			nil,
			parser.MustParseExpr("{count: 0}"),
			parser.MustParseExpr("{count: _acc.count + 1}"),
			[]document.Document{docFromJSON(`{"a": 10}`)},
			[]document.Document{docFromJSON(`{"count": 1}`)},
			false,
		},
		{
			"count/groupBy",
			parser.MustParseExpr("a % 2"),
			parser.MustParseExpr(`{count: 0, "group": _group}`),
			parser.MustParseExpr(`{count: _acc.count + 1, "group": _group}`),
			generateSeqDocs(10),
			[]document.Document{docFromJSON(`{"count": 5, "group": 0}`), docFromJSON(`{"count": 5, "group": 1}`)},
			false,
		},
		{
			"count/noInput",
			nil,
			parser.MustParseExpr(`{count: 0, "group": _group}`),
			parser.MustParseExpr(`{count: _acc.count + 1, "group": _group}`),
			nil,
			[]document.Document{docFromJSON(`{"count": 0, "group": null}`)},
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := stream.New(stream.NewDocumentIterator(test.in...))
			if test.groupBy != nil {
				s = s.Pipe(stream.GroupBy(test.groupBy))
			}
			s = s.Pipe(stream.Reduce(test.seed, test.acc))

			var got []document.Document
			err := s.Iterate(func(env *expr.Environment) error {
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
		require.Equal(t, `reduce({"count": 0}, {"count": _acc + 1})`, stream.Reduce(parser.MustParseExpr("{count: 0}"), parser.MustParseExpr("{count: _acc +1}")).String())
	})
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
			s := stream.New(stream.NewDocumentIterator(test.values...))
			if test.desc {
				s = s.Pipe(stream.SortReverse(test.sortExpr))
			} else {
				s = s.Pipe(stream.Sort(test.sortExpr))
			}

			var got []document.Document
			err := s.Iterate(func(env *expr.Environment) error {
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

			if test.out != nil {
				test.out.Outer = test.in
				tb, err := tx.GetTable("test")
				require.NoError(t, err)
				k, err := tb.EncodeValueToKey(document.NewIntegerValue(1))
				require.NoError(t, err)
				test.out.Doc.(*document.FieldBuffer).EncodedKey = k
			}

			ti := stream.TableInsert("test")
			err = ti.Bind(tx.Transaction, nil)
			require.NoError(t, err)

			op, err := ti.Op()
			require.NoError(t, err)
			env, err := op(test.in)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.out, env)
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, stream.TableInsert("test").String(), "tableInsert('test')")
	})
}
