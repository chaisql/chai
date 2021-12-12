package parser_test

import (
	"context"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/query"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/stream"
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
			stream.New(stream.TableScan("test")).
				Pipe(stream.PathsSet(document.Path(testutil.ParsePath(t, "a")), testutil.IntegerValue(1))).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.TableReplace("test")),
			false,
		},
		{"SET/With cond", "UPDATE test SET a = 1, b = 2 WHERE age = 10",
			stream.New(stream.TableScan("test")).
				Pipe(stream.DocsFilter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.PathsSet(document.Path(testutil.ParsePath(t, "a")), testutil.IntegerValue(1))).
				Pipe(stream.PathsSet(document.Path(testutil.ParsePath(t, "b")), parser.MustParseExpr("2"))).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.TableReplace("test")),
			false,
		},
		{"SET/No cond path with backquotes", "UPDATE test SET `   some \"path\" ` = 1",
			stream.New(stream.TableScan("test")).
				Pipe(stream.PathsSet(document.Path(testutil.ParsePath(t, "`   some \"path\" `")), testutil.IntegerValue(1))).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.TableReplace("test")),
			false,
		},
		{"SET/No cond nested path", "UPDATE test SET a.b = 1",
			stream.New(stream.TableScan("test")).
				Pipe(stream.PathsSet(document.Path(testutil.ParsePath(t, "a.b")), testutil.IntegerValue(1))).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.TableReplace("test")),
			false,
		},
		{"UNSET/No cond", "UPDATE test UNSET a",
			stream.New(stream.TableScan("test")).
				Pipe(stream.PathsUnset("a")).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.TableReplace("test")),
			false,
		},
		{"UNSET/With cond", "UPDATE test UNSET a, b WHERE age = 10",
			stream.New(stream.TableScan("test")).
				Pipe(stream.DocsFilter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.PathsUnset("a")).
				Pipe(stream.PathsUnset("b")).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.TableReplace("test")),
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
			db, cleanup := testutil.NewTestDB(t)
			defer cleanup()

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
