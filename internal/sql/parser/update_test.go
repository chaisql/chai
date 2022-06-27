package parser_test

import (
	"context"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/query"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/stream/docs"
	"github.com/genjidb/genji/internal/stream/path"
	"github.com/genjidb/genji/internal/stream/table"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
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
				Pipe(path.Set(document.Path(testutil.ParsePath(t, "a")), testutil.IntegerValue(1))).
				Pipe(table.Validate("test")).
				Pipe(table.Replace("test")).
				Pipe(stream.Discard()),
			false,
		},
		{"SET/With cond", "UPDATE test SET a = 1, b = 2 WHERE age = 10",
			stream.New(table.Scan("test")).
				Pipe(docs.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(path.Set(document.Path(testutil.ParsePath(t, "a")), testutil.IntegerValue(1))).
				Pipe(path.Set(document.Path(testutil.ParsePath(t, "b")), parser.MustParseExpr("2"))).
				Pipe(table.Validate("test")).
				Pipe(table.Replace("test")).
				Pipe(stream.Discard()),
			false,
		},
		{"SET/No cond path with backquotes", "UPDATE test SET `   some \"path\" ` = 1",
			stream.New(table.Scan("test")).
				Pipe(path.Set(document.Path(testutil.ParsePath(t, "`   some \"path\" `")), testutil.IntegerValue(1))).
				Pipe(table.Validate("test")).
				Pipe(table.Replace("test")).
				Pipe(stream.Discard()),
			false,
		},
		{"SET/No cond nested path", "UPDATE test SET a.b = 1",
			stream.New(table.Scan("test")).
				Pipe(path.Set(document.Path(testutil.ParsePath(t, "a.b")), testutil.IntegerValue(1))).
				Pipe(table.Validate("test")).
				Pipe(table.Replace("test")).
				Pipe(stream.Discard()),
			false,
		},
		{"UNSET/No cond", "UPDATE test UNSET a",
			stream.New(table.Scan("test")).
				Pipe(path.Unset("a")).
				Pipe(table.Validate("test")).
				Pipe(table.Replace("test")).
				Pipe(stream.Discard()),
			false,
		},
		{"UNSET/With cond", "UPDATE test UNSET a, b WHERE age = 10",
			stream.New(table.Scan("test")).
				Pipe(docs.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(path.Unset("a")).
				Pipe(path.Unset("b")).
				Pipe(table.Validate("test")).
				Pipe(table.Replace("test")).
				Pipe(stream.Discard()),
			false,
		},
		{"Trailing comma", "UPDATE test SET a = 1, WHERE age = 10", nil, true},
		{"No SET", "UPDATE test WHERE age = 10", nil, true},
		{"No pair", "UPDATE test SET WHERE age = 10", nil, true},
		{"query.Field only", "UPDATE test SET a WHERE age = 10", nil, true},
		{"No value", "UPDATE test SET a = WHERE age = 10", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := testutil.NewTestDB(t)

			testutil.MustExec(t, db, nil, "CREATE TABLE test")

			q, err := parser.ParseQuery(test.s)
			if test.errored {
				assert.Error(t, err)
				return
			}
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
