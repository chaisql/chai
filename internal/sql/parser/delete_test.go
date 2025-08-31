package parser_test

import (
	"testing"

	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/stream/table"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestParserDelete(t *testing.T) {
	db, tx, cleanup := testutil.NewTestTx(t)
	defer cleanup()

	testutil.MustExec(t, db, tx, "CREATE TABLE test(age int)")

	parseExpr := func(s string) expr.Expr {
		e := parser.MustParseExpr(s)
		err := statement.BindExpr(&statement.Context{DB: db, Conn: tx.Connection()}, "test", e)
		require.NoError(t, err)
		return e
	}

	tests := []struct {
		name     string
		s        string
		expected *stream.Stream
	}{
		{"NoCond", "DELETE FROM test", stream.New(table.Scan("test")).Pipe(table.Delete("test")).
			Pipe(stream.Discard())},
		{"WithCond", "DELETE FROM test WHERE age = 10",
			stream.New(table.Scan("test")).
				Pipe(rows.Filter(parseExpr("age = 10"))).
				Pipe(table.Delete("test")).
				Pipe(stream.Discard()),
		},
		{"WithOffset", "DELETE FROM test WHERE age = 10 OFFSET 20",
			stream.New(table.Scan("test")).
				Pipe(rows.Filter(parseExpr("age = 10"))).
				Pipe(rows.Skip(parseExpr("20"))).
				Pipe(table.Delete("test")).
				Pipe(stream.Discard()),
		},
		{"WithLimit", "DELETE FROM test LIMIT 10",
			stream.New(table.Scan("test")).
				Pipe(rows.Take(parseExpr("10"))).
				Pipe(table.Delete("test")).
				Pipe(stream.Discard()),
		},
		{"WithOrderByThenOffset", "DELETE FROM test WHERE age = 10 ORDER BY age OFFSET 20",
			stream.New(table.Scan("test")).
				Pipe(rows.Filter(parseExpr("age = 10"))).
				Pipe(rows.TempTreeSort(parseExpr("age"))).
				Pipe(rows.Skip(parseExpr("20"))).
				Pipe(table.Delete("test")).
				Pipe(stream.Discard()),
		},
		{"WithOrderByThenLimitThenOffset", "DELETE FROM test WHERE age = 10 ORDER BY age LIMIT 10 OFFSET 20",
			stream.New(table.Scan("test")).
				Pipe(rows.Filter(parseExpr("age = 10"))).
				Pipe(rows.TempTreeSort(parseExpr("age"))).
				Pipe(rows.Skip(parseExpr("20"))).
				Pipe(rows.Take(parseExpr("10"))).
				Pipe(table.Delete("test")).
				Pipe(stream.Discard()),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmts, err := parser.ParseQuery(test.s)
			require.NoError(t, err)

			require.Len(t, stmts, 1)
			stmt, err := stmts[0].(statement.Preparer).Prepare(&statement.Context{
				DB:   db,
				Conn: tx.Connection(),
			})
			require.NoError(t, err)

			require.Equal(t, test.expected.String(), stmt.(*statement.DeleteStmt).Stream.String())
		})
	}
}
