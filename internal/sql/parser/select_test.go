package parser_test

import (
	"context"
	"testing"

	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/expr/functions"
	"github.com/chaisql/chai/internal/query"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/stream/table"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestParserSelect(t *testing.T) {
	db, tx, cleanup := testutil.NewTestTx(t)
	defer cleanup()

	testutil.MustExec(t, db, tx, `
		CREATE TABLE test(a TEXT, b TEXT, age int);
		CREATE TABLE test1(age INT, a INT);
		CREATE TABLE test2(age INT, a INT);
		CREATE TABLE a(age INT, a INT);
		CREATE TABLE b(age INT, a INT);
		CREATE TABLE c(age INT, a INT);
		CREATE TABLE d(age INT, a INT);
	`,
	)

	parseExpr := func(s string, table ...string) expr.Expr {
		e := parser.MustParseExpr(s)
		tb := "test"
		if len(table) > 0 {
			tb = table[0]
		}
		err := statement.BindExpr(&statement.Context{DB: db, Tx: tx, Conn: tx.Connection()}, tb, e)
		require.NoError(t, err)
		return e
	}

	parseNamedExpr := func(t *testing.T, s string, name ...string) *expr.NamedExpr {
		ne := expr.NamedExpr{
			Expr:     parseExpr(s),
			ExprName: s,
		}

		if len(name) > 0 {
			ne.ExprName = name[0]
		}

		return &ne
	}

	tests := []struct {
		name     string
		s        string
		expected *stream.Stream
		readOnly bool
		mustFail bool
	}{
		{"NoTable", "SELECT 1",
			stream.New(rows.Project(parseNamedExpr(t, "1"))),
			true, false,
		},
		{"NoTableWithINOperator", "SELECT 1 in (1, 2), 3",
			stream.New(rows.Project(
				parseNamedExpr(t, "1 IN (1, 2)"),
				parseNamedExpr(t, "3"),
			)),
			true, false,
		},
		{"NoCond", "SELECT * FROM test",
			stream.New(table.Scan("test")).Pipe(rows.Project(expr.Wildcard{})),

			true, false,
		},
		{"Multiple Wildcards", "SELECT *, * FROM test",
			stream.New(table.Scan("test")).Pipe(rows.Project(expr.Wildcard{}, expr.Wildcard{})),
			true, false,
		},
		{"WithFields", "SELECT a, b FROM test",
			stream.New(table.Scan("test")).Pipe(rows.Project(parseNamedExpr(t, "a"), parseNamedExpr(t, "b"))),
			true, false,
		},
		{"WithAlias", "SELECT a AS A, b FROM test",
			stream.New(table.Scan("test")).Pipe(rows.Project(parseNamedExpr(t, "a", "A"), parseNamedExpr(t, "b"))),
			true, false,
		},
		{"WithFields and wildcard", "SELECT a, b, * FROM test",
			stream.New(table.Scan("test")).Pipe(rows.Project(parseNamedExpr(t, "a"), parseNamedExpr(t, "b"), expr.Wildcard{})),
			true, false,
		},
		{"WithExpr", "SELECT a    > 1 FROM test",
			stream.New(table.Scan("test")).Pipe(rows.Project(parseNamedExpr(t, "a > 1"))),
			true, false,
		},
		{"WithCond", "SELECT * FROM test WHERE age = 10",
			stream.New(table.Scan("test")).
				Pipe(rows.Filter(parseExpr("age = 10"))).
				Pipe(rows.Project(expr.Wildcard{})),
			true, false,
		},
		{"WithGroupBy", "SELECT a FROM test WHERE age = 10 GROUP BY a",
			stream.New(table.Scan("test")).
				Pipe(rows.Filter(parseExpr("age = 10"))).
				Pipe(rows.TempTreeSort(parseExpr("a"))).
				Pipe(rows.GroupAggregate(parseExpr("a"))).
				Pipe(rows.Project(&expr.NamedExpr{ExprName: "a", Expr: parseExpr("a")})),
			true, false,
		},
		{"WithOrderBy", "SELECT * FROM test WHERE age = 10 ORDER BY a",
			stream.New(table.Scan("test")).
				Pipe(rows.Filter(parseExpr("age = 10"))).
				Pipe(rows.Project(expr.Wildcard{})).
				Pipe(rows.TempTreeSort(parseExpr("a"))),
			true, false,
		},
		{"WithOrderBy ASC", "SELECT * FROM test WHERE age = 10 ORDER BY a ASC",
			stream.New(table.Scan("test")).
				Pipe(rows.Filter(parseExpr("age = 10"))).
				Pipe(rows.Project(expr.Wildcard{})).
				Pipe(rows.TempTreeSort(parseExpr("a"))),
			true, false,
		},
		{"WithOrderBy DESC", "SELECT * FROM test WHERE age = 10 ORDER BY a DESC",
			stream.New(table.Scan("test")).
				Pipe(rows.Filter(parseExpr("age = 10"))).
				Pipe(rows.Project(expr.Wildcard{})).
				Pipe(rows.TempTreeSortReverse(parseExpr("a"))),
			true, false,
		},
		{"WithLimit", "SELECT * FROM test WHERE age = 10 LIMIT 20",
			stream.New(table.Scan("test")).
				Pipe(rows.Filter(parseExpr("age = 10"))).
				Pipe(rows.Project(expr.Wildcard{})).
				Pipe(rows.Take(parseExpr("20"))),
			true, false,
		},
		{"WithOffset", "SELECT * FROM test WHERE age = 10 OFFSET 20",
			stream.New(table.Scan("test")).
				Pipe(rows.Filter(parseExpr("age = 10"))).
				Pipe(rows.Project(expr.Wildcard{})).
				Pipe(rows.Skip(parseExpr("20"))),
			true, false,
		},
		{"WithLimitThenOffset", "SELECT * FROM test WHERE age = 10 LIMIT 10 OFFSET 20",
			stream.New(table.Scan("test")).
				Pipe(rows.Filter(parseExpr("age = 10"))).
				Pipe(rows.Project(expr.Wildcard{})).
				Pipe(rows.Skip(parseExpr("20"))).
				Pipe(rows.Take(parseExpr("10"))),
			true, false,
		},
		{"WithOffsetThenLimit", "SELECT * FROM test WHERE age = 10 OFFSET 20 LIMIT 10", nil, true, true},
		{"With aggregation function", "SELECT COUNT(*) FROM test",
			stream.New(table.Scan("test")).
				Pipe(rows.GroupAggregate(nil, functions.NewCount(expr.Wildcard{}))).
				Pipe(rows.Project(parseNamedExpr(t, "COUNT(*)"))),
			true, false},
		{"With NEXT VALUE FOR", "SELECT NEXT VALUE FOR foo FROM test",
			stream.New(table.Scan("test")).
				Pipe(rows.Project(parseNamedExpr(t, "NEXT VALUE FOR foo"))),
			false, false},
		{"WithUnionAll", "SELECT * FROM test1 UNION ALL SELECT * FROM test2",
			stream.New(stream.Concat(
				stream.New(table.Scan("test1")).Pipe(rows.Project(expr.Wildcard{})),
				stream.New(table.Scan("test2")).Pipe(rows.Project(expr.Wildcard{})),
			)),
			true, false,
		},
		{"CondWithUnionAll", "SELECT * FROM test1 WHERE age = 10 UNION ALL SELECT * FROM test2",
			stream.New(stream.Concat(
				stream.New(table.Scan("test1")).
					Pipe(rows.Filter(parseExpr("age = 10", "test1"))).
					Pipe(rows.Project(expr.Wildcard{})),
				stream.New(table.Scan("test2")).
					Pipe(rows.Project(expr.Wildcard{})),
			)),
			true, false,
		},
		{"WithUnionAllAfterOrderBy", "SELECT * FROM test1 ORDER BY a UNION ALL SELECT * FROM test2",
			nil,
			true, true,
		},
		{"WithUnionAllAfterLimit", "SELECT * FROM test1 LIMIT 10 UNION ALL SELECT * FROM test2",
			nil,
			true, true,
		},
		{"WithUnionAllAfterOffset", "SELECT * FROM test1 OFFSET 10 UNION ALL SELECT * FROM test2",
			nil,
			true, true,
		},
		{"WithUnionAllAndOrderBy", "SELECT * FROM test1 UNION ALL SELECT * FROM test2 ORDER BY a",
			stream.New(stream.Concat(
				stream.New(table.Scan("test1")).
					Pipe(rows.Project(expr.Wildcard{})),
				stream.New(table.Scan("test2")).
					Pipe(rows.Project(expr.Wildcard{})),
			)).Pipe(rows.TempTreeSort(parseExpr("a", "test1"))),
			true, false,
		},
		{"WithUnionAllAndLimit", "SELECT * FROM test1 UNION ALL SELECT * FROM test2 LIMIT 10",
			stream.New(stream.Concat(
				stream.New(table.Scan("test1")).
					Pipe(rows.Project(expr.Wildcard{})),
				stream.New(table.Scan("test2")).
					Pipe(rows.Project(expr.Wildcard{})),
			)).Pipe(rows.Take(parseExpr("10"))),
			true, false,
		},
		{"WithUnionAllAndOffset", "SELECT * FROM test1 UNION ALL SELECT * FROM test2 OFFSET 20",
			stream.New(stream.Concat(
				stream.New(table.Scan("test1")).
					Pipe(rows.Project(expr.Wildcard{})),
				stream.New(table.Scan("test2")).
					Pipe(rows.Project(expr.Wildcard{})),
			)).Pipe(rows.Skip(parseExpr("20"))),
			true, false,
		},
		{"WithUnionAllAndOrderByAndLimitAndOffset", "SELECT * FROM test1 UNION ALL SELECT * FROM test2 ORDER BY a LIMIT 10 OFFSET 20",
			stream.New(stream.Concat(
				stream.New(table.Scan("test1")).
					Pipe(rows.Project(expr.Wildcard{})),
				stream.New(table.Scan("test2")).
					Pipe(rows.Project(expr.Wildcard{})),
			)).Pipe(rows.TempTreeSort(parseExpr("a", "test1"))).Pipe(rows.Skip(parseExpr("20"))).Pipe(rows.Take(parseExpr("10"))),
			true, false,
		},

		{"WithUnion", "SELECT * FROM test1 UNION SELECT * FROM test2",
			stream.New(stream.Union(
				stream.New(table.Scan("test1")).
					Pipe(rows.Project(expr.Wildcard{})),
				stream.New(table.Scan("test2")).
					Pipe(rows.Project(expr.Wildcard{})),
			)),
			true, false,
		},
		{"CondWithUnion", "SELECT * FROM test1 WHERE age = 10 UNION SELECT * FROM test2",
			stream.New(stream.Union(
				stream.New(table.Scan("test1")).
					Pipe(rows.Filter(parseExpr("age = 10", "test1"))).
					Pipe(rows.Project(expr.Wildcard{})),
				stream.New(table.Scan("test2")).
					Pipe(rows.Project(expr.Wildcard{})),
			)),
			true, false,
		},
		{"WithUnionAfterOrderBy", "SELECT * FROM test1 ORDER BY a UNION SELECT * FROM test2",
			nil,
			true, true,
		},
		{"WithUnionAfterLimit", "SELECT * FROM test1 LIMIT 10 UNION SELECT * FROM test2",
			nil,
			true, true,
		},
		{"WithUnionAfterOffset", "SELECT * FROM test1 OFFSET 10 UNION SELECT * FROM test2",
			nil,
			true, true,
		},
		{"WithUnionAndOrderBy", "SELECT * FROM test1 UNION SELECT * FROM test2 ORDER BY a",
			stream.New(stream.Union(
				stream.New(table.Scan("test1")).
					Pipe(rows.Project(expr.Wildcard{})),
				stream.New(table.Scan("test2")).
					Pipe(rows.Project(expr.Wildcard{})),
			)).Pipe(rows.TempTreeSort(parseExpr("a", "test1"))),
			true, false,
		},
		{"WithUnionAndLimit", "SELECT * FROM test1 UNION SELECT * FROM test2 LIMIT 10",
			stream.New(stream.Union(
				stream.New(table.Scan("test1")).
					Pipe(rows.Project(expr.Wildcard{})),
				stream.New(table.Scan("test2")).
					Pipe(rows.Project(expr.Wildcard{})),
			)).Pipe(rows.Take(parseExpr("10"))),
			true, false,
		},
		{"WithUnionAndOffset", "SELECT * FROM test1 UNION SELECT * FROM test2 OFFSET 20",
			stream.New(stream.Union(
				stream.New(table.Scan("test1")).
					Pipe(rows.Project(expr.Wildcard{})),
				stream.New(table.Scan("test2")).
					Pipe(rows.Project(expr.Wildcard{})),
			)).Pipe(rows.Skip(parseExpr("20"))),
			true, false,
		},
		{"WithUnionAndOrderByAndLimitAndOffset", "SELECT * FROM test1 UNION SELECT * FROM test2 ORDER BY a LIMIT 10 OFFSET 20",
			stream.New(stream.Union(
				stream.New(table.Scan("test1")).
					Pipe(rows.Project(expr.Wildcard{})),
				stream.New(table.Scan("test2")).
					Pipe(rows.Project(expr.Wildcard{})),
			)).Pipe(rows.TempTreeSort(parseExpr("a", "test1"))).Pipe(rows.Skip(parseExpr("20"))).Pipe(rows.Take(parseExpr("10"))),
			true, false,
		},
		{"WithMultipleCompoundOps/1", "SELECT * FROM a UNION ALL SELECT * FROM b UNION ALL SELECT * FROM c",
			stream.New(stream.Concat(
				stream.New(table.Scan("a")).
					Pipe(rows.Project(expr.Wildcard{})),
				stream.New(table.Scan("b")).
					Pipe(rows.Project(expr.Wildcard{})),
				stream.New(table.Scan("c")).
					Pipe(rows.Project(expr.Wildcard{})),
			)),
			true, false,
		},
		{"WithMultipleCompoundOps/2", "SELECT * FROM a UNION ALL SELECT * FROM b UNION SELECT * FROM c",
			stream.New(stream.Union(
				stream.New(stream.Concat(
					stream.New(table.Scan("a")).
						Pipe(rows.Project(expr.Wildcard{})),
					stream.New(table.Scan("b")).
						Pipe(rows.Project(expr.Wildcard{})),
				)),
				stream.New(table.Scan("c")).
					Pipe(rows.Project(expr.Wildcard{})),
			)),
			true, false,
		},
		{"WithMultipleCompoundOps/2", "SELECT * FROM a UNION ALL SELECT * FROM b UNION ALL SELECT * FROM c UNION SELECT * FROM d",
			stream.New(stream.Union(
				stream.New(stream.Concat(
					stream.New(table.Scan("a")).
						Pipe(rows.Project(expr.Wildcard{})),
					stream.New(table.Scan("b")).
						Pipe(rows.Project(expr.Wildcard{})),
					stream.New(table.Scan("c")).
						Pipe(rows.Project(expr.Wildcard{})),
				)),
				stream.New(table.Scan("d")).
					Pipe(rows.Project(expr.Wildcard{})),
			)),
			true, false,
		},
		{"WithMultipleCompoundOps/3", "SELECT * FROM a UNION ALL SELECT * FROM b UNION SELECT * FROM c UNION SELECT * FROM d",
			stream.New(stream.Union(
				stream.New(stream.Concat(
					stream.New(table.Scan("a")).
						Pipe(rows.Project(expr.Wildcard{})),
					stream.New(table.Scan("b")).
						Pipe(rows.Project(expr.Wildcard{})),
				)),
				stream.New(table.Scan("c")).
					Pipe(rows.Project(expr.Wildcard{})),
				stream.New(table.Scan("d")).
					Pipe(rows.Project(expr.Wildcard{})),
			)),
			true, false,
		},
		{"WithMultipleCompoundOps/4", "SELECT * FROM a UNION ALL SELECT * FROM b UNION SELECT * FROM c UNION ALL SELECT * FROM d",
			stream.New(stream.Concat(
				stream.New(stream.Union(
					stream.New(stream.Concat(
						stream.New(table.Scan("a")).
							Pipe(rows.Project(expr.Wildcard{})),
						stream.New(table.Scan("b")).
							Pipe(rows.Project(expr.Wildcard{})),
					)),
					stream.New(table.Scan("c")).
						Pipe(rows.Project(expr.Wildcard{})),
				)),
				stream.New(table.Scan("d")).
					Pipe(rows.Project(expr.Wildcard{})),
			)),
			true, false,
		},
		{"WithMultipleCompoundOpsAndNextValueFor/4", "SELECT * FROM a UNION ALL SELECT * FROM b UNION SELECT * FROM c UNION ALL SELECT NEXT VALUE FOR foo FROM d",
			stream.New(stream.Concat(
				stream.New(stream.Union(
					stream.New(stream.Concat(
						stream.New(table.Scan("a")).
							Pipe(rows.Project(expr.Wildcard{})),
						stream.New(table.Scan("b")).
							Pipe(rows.Project(expr.Wildcard{})),
					)),
					stream.New(table.Scan("c")).
						Pipe(rows.Project(expr.Wildcard{})),
				)),
				stream.New(table.Scan("d")).Pipe(rows.Project(parseNamedExpr(t, "NEXT VALUE FOR foo"))),
			)),
			false, false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parser.ParseQuery(test.s)
			if test.mustFail {
				require.Error(t, err)
				return
			}

			err = q.Prepare(&query.Context{
				Ctx:  context.Background(),
				DB:   db,
				Conn: tx.Connection(),
			})
			require.NoError(t, err)

			require.Len(t, q.Statements, 1)
			require.EqualValues(t, &statement.PreparedStreamStmt{ReadOnly: test.readOnly, Stream: test.expected}, q.Statements[0].(*statement.PreparedStreamStmt))
		})
	}
}

func BenchmarkSelect(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = parser.ParseQuery("SELECT a, b AS `foo` FROM `some table` WHERE d.e[100] >= 12 AND c.d IN ([1, true], [2, false]) GROUP BY d.e[0] LIMIT 10 + 10 OFFSET 20 - 20 ORDER BY d DESC")
	}
}
