package parser_test

import (
	"context"
	"testing"

	"github.com/chaisql/chai/internal/query"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/stream/table"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/stretchr/testify/require"
)

func TestParserDelete(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected *stream.Stream
	}{
		{"NoCond", "DELETE FROM test", stream.New(table.Scan("test")).Pipe(table.Delete("test")).
			Pipe(stream.Discard())},
		{"WithCond", "DELETE FROM test WHERE age = 10",
			stream.New(table.Scan("test")).
				Pipe(rows.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(table.Delete("test")).
				Pipe(stream.Discard()),
		},
		{"WithOffset", "DELETE FROM test WHERE age = 10 OFFSET 20",
			stream.New(table.Scan("test")).
				Pipe(rows.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(rows.Skip(parser.MustParseExpr("20"))).
				Pipe(table.Delete("test")).
				Pipe(stream.Discard()),
		},
		{"WithLimit", "DELETE FROM test LIMIT 10",
			stream.New(table.Scan("test")).
				Pipe(rows.Take(parser.MustParseExpr("10"))).
				Pipe(table.Delete("test")).
				Pipe(stream.Discard()),
		},
		{"WithOrderByThenOffset", "DELETE FROM test WHERE age = 10 ORDER BY age OFFSET 20",
			stream.New(table.Scan("test")).
				Pipe(rows.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(rows.TempTreeSort(parser.MustParseExpr("age"))).
				Pipe(rows.Skip(parser.MustParseExpr("20"))).
				Pipe(table.Delete("test")).
				Pipe(stream.Discard()),
		},
		{"WithOrderByThenLimitThenOffset", "DELETE FROM test WHERE age = 10 ORDER BY age LIMIT 10 OFFSET 20",
			stream.New(table.Scan("test")).
				Pipe(rows.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(rows.TempTreeSort(parser.MustParseExpr("age"))).
				Pipe(rows.Skip(parser.MustParseExpr("20"))).
				Pipe(rows.Take(parser.MustParseExpr("10"))).
				Pipe(table.Delete("test")).
				Pipe(stream.Discard()),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := testutil.NewTestDB(t)

			testutil.MustExec(t, db, nil, "CREATE TABLE test")

			q, err := parser.ParseQuery(test.s)
			assert.NoError(t, err)

			err = q.Prepare(&query.Context{
				Ctx: context.Background(),
				DB:  db,
			})
			assert.NoError(t, err)

			require.Len(t, q.Statements, 1)
			require.EqualValues(t, &statement.PreparedStreamStmt{Stream: test.expected}, q.Statements[0].(*statement.PreparedStreamStmt))
		})
	}
}
