package parser_test

import (
	"testing"

	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/query"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/sql/parser"
	"github.com/stretchr/testify/require"
)

func TestParserExplain(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Statement
		errored  bool
	}{
		{"Explain create table", "EXPLAIN SELECT * FROM test", &query.ExplainStmt{Statement: &query.StreamStmt{Stream: stream.New(stream.SeqScan("test")).Pipe(stream.Project(expr.Wildcard{})), ReadOnly: true}}, false},
		{"Multiple Explains", "EXPLAIN EXPLAIN CREATE TABLE test", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parser.ParseQuery(test.s)
			if test.errored {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}
