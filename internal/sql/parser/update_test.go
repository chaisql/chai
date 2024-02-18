package parser_test

import (
	"context"
	"testing"

	"github.com/chaisql/chai/internal/query"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/path"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/stream/table"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestParserUpdate(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected *stream.Stream
		errored  bool
	}{
		{"SET/No cond", "UPDATE test SET a = 1",
			stream.New(table.Scan("test")).
				Pipe(path.Set("a", testutil.IntegerValue(1))).
				Pipe(table.Validate("test")).
				Pipe(table.Replace("test")).
				Pipe(stream.Discard()),
			false,
		},
		{"SET/With cond", "UPDATE test SET a = 1, b = 2 WHERE a = 10",
			stream.New(table.Scan("test")).
				Pipe(rows.Filter(parser.MustParseExpr("a = 10"))).
				Pipe(path.Set("a", testutil.IntegerValue(1))).
				Pipe(path.Set("b", parser.MustParseExpr("2"))).
				Pipe(table.Validate("test")).
				Pipe(table.Replace("test")).
				Pipe(stream.Discard()),
			false,
		},
		{"Trailing comma", "UPDATE test SET a = 1, WHERE a = 10", nil, true},
		{"No SET", "UPDATE test WHERE a = 10", nil, true},
		{"No pair", "UPDATE test SET WHERE a = 10", nil, true},
		{"query.Field only", "UPDATE test SET a WHERE a = 10", nil, true},
		{"No value", "UPDATE test SET a = WHERE a = 10", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, "CREATE TABLE test(a INT, b TEXT)")

			q, err := parser.ParseQuery(test.s)
			if test.errored {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			err = q.Prepare(&query.Context{
				Ctx:  context.Background(),
				DB:   db,
				Conn: tx.Connection(),
			})
			require.NoError(t, err)

			require.Len(t, q.Statements, 1)
			require.EqualValues(t, &statement.PreparedStreamStmt{Stream: test.expected}, q.Statements[0].(*statement.PreparedStreamStmt))
		})
	}
}
