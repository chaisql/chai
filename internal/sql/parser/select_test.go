package parser_test

import (
	"testing"

	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/expr/functions"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestParserSelect(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected *stream.Stream
		mustFail bool
	}{
		{"NoTable", "SELECT 1",
			stream.New(stream.Expressions(
				&expr.KVPairs{
					Pairs: []expr.KVPair{
						{K: "1", V: testutil.ParseNamedExpr(t, "1")},
					},
				},
			)),
			false,
		},
		{"NoTableWithTuple", "SELECT (1, 2)",
			stream.New(stream.Expressions(
				&expr.KVPairs{
					Pairs: []expr.KVPair{
						{K: "[1, 2]", V: testutil.ParseNamedExpr(t, "[1, 2]")},
					},
				},
			)),
			false,
		},
		{"NoTableWithBrackets", "SELECT [1, 2]",
			stream.New(stream.Expressions(
				&expr.KVPairs{
					Pairs: []expr.KVPair{
						{K: "[1, 2]", V: testutil.ParseNamedExpr(t, "[1, 2]")},
					},
				},
			)),
			false,
		},
		{"NoTableWithINOperator", "SELECT 1 in (1, 2), 3",
			stream.New(stream.Expressions(
				&expr.KVPairs{
					Pairs: []expr.KVPair{
						{K: "1 IN [1, 2]", V: testutil.ParseNamedExpr(t, "1 IN [1, 2]")},
						{K: "3", V: testutil.ParseNamedExpr(t, "3")},
					},
				},
			)),
			false,
		},
		{"NoCond", "SELECT * FROM test",
			stream.New(stream.SeqScan("test")).Pipe(stream.Project(expr.Wildcard{})),
			false,
		},
		{"WithFields", "SELECT a, b FROM test",
			stream.New(stream.SeqScan("test")).Pipe(stream.Project(testutil.ParseNamedExpr(t, "a"), testutil.ParseNamedExpr(t, "b"))),
			false,
		},
		{"WithFieldsWithQuotes", "SELECT `long \"path\"` FROM test",
			stream.New(stream.SeqScan("test")).Pipe(stream.Project(testutil.ParseNamedExpr(t, "`long \"path\"`", "long \"path\""))),
			false,
		},
		{"WithAlias", "SELECT a AS A, b FROM test",
			stream.New(stream.SeqScan("test")).Pipe(stream.Project(testutil.ParseNamedExpr(t, "a", "A"), testutil.ParseNamedExpr(t, "b"))),
			false,
		},
		{"WithFields and wildcard", "SELECT a, b, * FROM test",
			stream.New(stream.SeqScan("test")).Pipe(stream.Project(testutil.ParseNamedExpr(t, "a"), testutil.ParseNamedExpr(t, "b"), expr.Wildcard{})),
			false,
		},
		{"WithExpr", "SELECT a    > 1 FROM test",
			stream.New(stream.SeqScan("test")).Pipe(stream.Project(testutil.ParseNamedExpr(t, "a > 1", "a > 1"))),
			false,
		},
		{"WithCond", "SELECT * FROM test WHERE age = 10",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.Project(expr.Wildcard{})),
			false,
		},
		{"WithGroupBy", "SELECT a.b.c FROM test WHERE age = 10 GROUP BY a.b.c",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.GroupBy(parser.MustParseExpr("a.b.c"))).
				Pipe(stream.HashAggregate()).
				Pipe(stream.Project(testutil.ParseNamedExpr(t, "a.b.c"))),
			false,
		},
		{"WithOrderBy", "SELECT * FROM test WHERE age = 10 ORDER BY a.b.c",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.Project(expr.Wildcard{})).
				Pipe(stream.Sort(testutil.ParsePath(t, "a.b.c"))),
			false,
		},
		{"WithOrderBy ASC", "SELECT * FROM test WHERE age = 10 ORDER BY a.b.c ASC",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.Project(expr.Wildcard{})).
				Pipe(stream.Sort(testutil.ParsePath(t, "a.b.c"))),
			false,
		},
		{"WithOrderBy DESC", "SELECT * FROM test WHERE age = 10 ORDER BY a.b.c DESC",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.Project(expr.Wildcard{})).
				Pipe(stream.SortReverse(testutil.ParsePath(t, "a.b.c"))),
			false,
		},
		{"WithLimit", "SELECT * FROM test WHERE age = 10 LIMIT 20",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.Project(expr.Wildcard{})).
				Pipe(stream.Take(20)),
			false,
		},
		{"WithOffset", "SELECT * FROM test WHERE age = 10 OFFSET 20",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.Project(expr.Wildcard{})).
				Pipe(stream.Skip(20)),
			false,
		},
		{"WithLimitThenOffset", "SELECT * FROM test WHERE age = 10 LIMIT 10 OFFSET 20",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.Project(expr.Wildcard{})).
				Pipe(stream.Skip(20)).
				Pipe(stream.Take(10)),
			false,
		},
		{"WithOffsetThenLimit", "SELECT * FROM test WHERE age = 10 OFFSET 20 LIMIT 10", nil, true},
		{"With aggregation function", "SELECT COUNT(*) FROM test",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.HashAggregate(&functions.Count{Wildcard: true})).
				Pipe(stream.Project(testutil.ParseNamedExpr(t, "COUNT(*)"))),
			false},
		{"WithUnionAll", "SELECT * FROM test1 UNION ALL SELECT * FROM test2",
			stream.New(stream.Concat(
				stream.New(stream.SeqScan("test1")).Pipe(stream.Project(expr.Wildcard{})),
				stream.New(stream.SeqScan("test2")).Pipe(stream.Project(expr.Wildcard{})),
			)),
			false,
		},
		{"CondWithUnionAll", "SELECT * FROM test1 WHERE age = 10 UNION ALL SELECT * FROM test2",
			stream.New(stream.Concat(
				stream.New(stream.SeqScan("test1")).
					Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
					Pipe(stream.Project(expr.Wildcard{})),
				stream.New(stream.SeqScan("test2")).Pipe(stream.Project(expr.Wildcard{})),
			)),
			false,
		},
		{"WithUnionAllThenUnionAll", "SELECT * FROM test1 UNION ALL SELECT * FROM test2 UNION ALL SELECT * FROM test3",
			stream.New(stream.Concat(
				stream.New(stream.SeqScan("test1")).Pipe(stream.Project(expr.Wildcard{})),
				stream.New(stream.Concat(
					stream.New(stream.SeqScan("test2")).Pipe(stream.Project(expr.Wildcard{})),
					stream.New(stream.SeqScan("test3")).Pipe(stream.Project(expr.Wildcard{})),
				)))),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parser.ParseQuery(test.s)
			if !test.mustFail {
				require.NoError(t, err)
				require.Len(t, q.Statements, 1)
				require.EqualValues(t, &statement.StreamStmt{Stream: test.expected, ReadOnly: true}, q.Statements[0].(*statement.StreamStmt))
			} else {
				require.Error(t, err)
			}
		})
	}
}

func BenchmarkSelect(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = parser.ParseQuery("SELECT a, b.c[100].d AS `foo` FROM `some table` WHERE d.e[100] >= 12 AND c.d IN ([1, true], [2, false]) GROUP BY d.e[0] LIMIT 10 + 10 OFFSET 20 - 20 ORDER BY d DESC")
	}
}
