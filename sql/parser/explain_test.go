package parser

import (
	"testing"

	"github.com/genjidb/genji/query"
	"github.com/genjidb/genji/sql/planner"
	"github.com/stretchr/testify/require"
)

func TestParserExplain(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Statement
		errored  bool
	}{
		{"Explain create table", "EXPLAIN CREATE TABLE test", &planner.ExplainStmt{Statement: query.CreateTableStmt{TableName: "test"}}, false},
		{"Multiple Explains", "EXPLAIN EXPLAIN CREATE TABLE test", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := ParseQuery(test.s)
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
