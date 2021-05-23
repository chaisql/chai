package parser_test

import (
	"testing"

	"github.com/genjidb/genji/internal/query"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/stretchr/testify/require"
)

func TestParserDrop(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Statement
		errored  bool
	}{
		{"Drop table", "DROP TABLE test", query.DropTableStmt{TableName: "test"}, false},
		{"Drop table If not exists", "DROP TABLE IF EXISTS test", query.DropTableStmt{TableName: "test", IfExists: true}, false},
		{"Drop index", "DROP INDEX test", query.DropIndexStmt{IndexName: "test"}, false},
		{"Drop index if exists", "DROP INDEX IF EXISTS test", query.DropIndexStmt{IndexName: "test", IfExists: true}, false},
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
