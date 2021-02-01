package parser

import (
	"testing"

	"github.com/genjidb/genji/sql/planner"
	"github.com/genjidb/genji/stream"
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
				Pipe(stream.Filter(MustParseExpr("age = 10"))).
				Pipe(stream.TableDelete("test")),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := ParseQuery(test.s)
			require.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, &planner.Statement{Stream: test.expected}, q.Statements[0])
		})
	}
}
