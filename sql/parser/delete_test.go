package parser_test

import (
	"testing"

	"github.com/genjidb/genji/internal/query"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/sql/parser"
	"github.com/stretchr/testify/require"
)

func TestParserDelete(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected *stream.Stream
	}{
		{"NoCond", "DELETE FROM test", stream.New(stream.SeqScan("test")).Pipe(stream.TableDelete("test"))},
		{"WithCond", "DELETE FROM test WHERE age = 10",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.TableDelete("test")),
		},
		{"WithOffset", "DELETE FROM test WHERE age = 10 OFFSET 20",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.Skip(20)).
				Pipe(stream.TableDelete("test")),
		},
		{"WithLimit", "DELETE FROM test LIMIT 10",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.Take(10)).
				Pipe(stream.TableDelete("test")),
		},
		{"WithOrderByThenOffset", "DELETE FROM test WHERE age = 10 ORDER BY age OFFSET 20",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.Sort(parser.MustParseExpr("age"))).
				Pipe(stream.Skip(20)).
				Pipe(stream.TableDelete("test")),
		},
		{"WithOrderByThenLimitThenOffset", "DELETE FROM test WHERE age = 10 ORDER BY age LIMIT 10 OFFSET 20",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.Sort(parser.MustParseExpr("age"))).
				Pipe(stream.Skip(20)).
				Pipe(stream.Take(10)).
				Pipe(stream.TableDelete("test")),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parser.ParseQuery(test.s)
			require.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, &query.StreamStmt{Stream: test.expected}, q.Statements[0])
		})
	}
}
