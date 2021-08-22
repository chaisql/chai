package parser_test

import (
	"testing"

	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/stretchr/testify/require"
)

func TestParserExplain(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected statement.Statement
		errored  bool
	}{
		{"Explain create table", "EXPLAIN SELECT * FROM test", &statement.ExplainStmt{Statement: &statement.StreamStmt{
			ReadOnly: true,
			Stream:   stream.New(stream.SeqScan("test")).Pipe(stream.Project(expr.Wildcard{})),
		}}, false},
		{"Multiple Explains", "EXPLAIN EXPLAIN CREATE TABLE test", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parser.ParseQuery(test.s)
			if test.errored {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}
