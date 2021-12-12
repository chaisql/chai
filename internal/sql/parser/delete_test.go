package parser_test

import (
	"context"
	"testing"

	"github.com/genjidb/genji/internal/query"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/stretchr/testify/require"
)

func TestParserDelete(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected *stream.Stream
	}{
		{"NoCond", "DELETE FROM test", stream.New(stream.TableScan("test")).Pipe(stream.TableDelete("test"))},
		{"WithCond", "DELETE FROM test WHERE age = 10",
			stream.New(stream.TableScan("test")).
				Pipe(stream.DocsFilter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.TableDelete("test")),
		},
		{"WithOffset", "DELETE FROM test WHERE age = 10 OFFSET 20",
			stream.New(stream.TableScan("test")).
				Pipe(stream.DocsFilter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.DocsSkip(20)).
				Pipe(stream.TableDelete("test")),
		},
		{"WithLimit", "DELETE FROM test LIMIT 10",
			stream.New(stream.TableScan("test")).
				Pipe(stream.DocsTake(10)).
				Pipe(stream.TableDelete("test")),
		},
		{"WithOrderByThenOffset", "DELETE FROM test WHERE age = 10 ORDER BY age OFFSET 20",
			stream.New(stream.TableScan("test")).
				Pipe(stream.DocsFilter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.DocsTempTreeSort(parser.MustParseExpr("age"))).
				Pipe(stream.DocsSkip(20)).
				Pipe(stream.TableDelete("test")),
		},
		{"WithOrderByThenLimitThenOffset", "DELETE FROM test WHERE age = 10 ORDER BY age LIMIT 10 OFFSET 20",
			stream.New(stream.TableScan("test")).
				Pipe(stream.DocsFilter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.DocsTempTreeSort(parser.MustParseExpr("age"))).
				Pipe(stream.DocsSkip(20)).
				Pipe(stream.DocsTake(10)).
				Pipe(stream.TableDelete("test")),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, cleanup := testutil.NewTestDB(t)
			defer cleanup()

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
