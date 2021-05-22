package parser_test

import (
	"testing"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/query"
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
		{"Explain create table", "EXPLAIN CREATE TABLE test", &query.ExplainStmt{Statement: query.CreateTableStmt{Info: database.TableInfo{TableName: "test"}}}, false},
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
