package parser_test

import (
	"testing"

	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/stretchr/testify/require"
)

func TestParserDrop(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected statement.Statement
		errored  bool
	}{
		{"Drop table", "DROP TABLE test", &statement.DropTableStmt{TableName: "test"}, false},
		{"Drop table If not exists", "DROP TABLE IF EXISTS test", &statement.DropTableStmt{TableName: "test", IfExists: true}, false},
		{"Drop index", "DROP INDEX test", &statement.DropIndexStmt{IndexName: "test"}, false},
		{"Drop index if exists", "DROP INDEX IF EXISTS test", &statement.DropIndexStmt{IndexName: "test", IfExists: true}, false},
		{"Drop index", "DROP SEQUENCE test", &statement.DropSequenceStmt{SequenceName: "test"}, false},
		{"Drop index if exists", "DROP SEQUENCE IF EXISTS test", &statement.DropSequenceStmt{SequenceName: "test", IfExists: true}, false},
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
