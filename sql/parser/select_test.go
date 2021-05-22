package parser_test

import (
	"testing"

	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/query"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/sql/parser"
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
			stream.New(stream.Project(testutil.ParseNamedExpr(t, "1"))),
			false,
		},
		{"NoTable/path", "SELECT a",
			nil,
			true,
		},
		{"NoTable/wildcard", "SELECT *",
			nil,
			true,
		},
		{"Wildcard with no FORM", "SELECT *", nil, true},
		{"NoTableWithTuple", "SELECT (1, 2)",
			stream.New(stream.Project(testutil.ParseNamedExpr(t, "(1, 2)"))),
			false,
		},
		{"NoTableWithBrackets", "SELECT [1, 2]",
			stream.New(stream.Project(testutil.ParseNamedExpr(t, "[1, 2]"))),
			false,
		},
		{"NoTableWithINOperator", "SELECT 1 in (1, 2), 3",
			stream.New(stream.Project(
				testutil.ParseNamedExpr(t, "1 in (1, 2)"),
				testutil.ParseNamedExpr(t, "3"),
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
			stream.New(stream.SeqScan("test")).Pipe(stream.Project(testutil.ParseNamedExpr(t, "a > 1", "a    > 1"))),
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
		{"With Invalid GroupBy: Wildcard", "SELECT * FROM test WHERE age = 10 GROUP BY a.b.c", nil, true},
		{"With Invalid GroupBy: a.b", "SELECT a.b FROM test WHERE age = 10 GROUP BY a.b.c", nil, true},
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
				Pipe(stream.HashAggregate(&expr.CountFunc{Wildcard: true})).
				Pipe(stream.Project(testutil.ParseNamedExpr(t, "COUNT(*)"))),
			false},
		{"Invalid use of MIN() aggregator", "SELECT * FROM test LIMIT min(0)", nil, true},
		{"Invalid use of COUNT() aggregator", "SELECT * FROM test OFFSET x(*)", nil, true},
		{"Invalid use of MAX() aggregator", "SELECT * FROM test LIMIT max(0)", nil, true},
		{"Invalid use of SUM() aggregator", "SELECT * FROM test LIMIT sum(0)", nil, true},
		{"Invalid use of AVG() aggregator", "SELECT * FROM test LIMIT avg(0)", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parser.ParseQuery(test.s)
			if !test.mustFail {
				require.NoError(t, err)
				require.Len(t, q.Statements, 1)
				require.EqualValues(t, &query.StreamStmt{Stream: test.expected, ReadOnly: true}, q.Statements[0])
			} else {
				require.Error(t, err)
			}
		})
	}
}
