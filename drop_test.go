package genji

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParserDrop(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected statement
		errored  bool
	}{
		{"Drop table", "DROP TABLE test", dropTableStmt{tableName: "test"}, false},
		{"Drop table If not exists", "DROP TABLE IF EXISTS test", dropTableStmt{tableName: "test", ifExists: true}, false},
		{"Drop index", "DROP INDEX test", dropIndexStmt{indexName: "test"}, false},
		{"Drop index if exists", "DROP INDEX IF EXISTS test", dropIndexStmt{indexName: "test", ifExists: true}, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parseQuery(test.s)
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
