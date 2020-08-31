package parser

import (
	"testing"

	"github.com/genjidb/genji/sql/query"
	"github.com/stretchr/testify/require"
)

func TestParserTransactions(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Statement
		errored  bool
	}{
		{"Begin", "BEGIN", query.BeginStmt{}, false},
		{"Begin transaction", "BEGIN TRANSACTION", query.BeginStmt{}, false},
		{"Rollback", "ROLLBACK", query.RollbackStmt{}, false},
		{"Rollback transaction", "ROLLBACK TRANSACTION", query.RollbackStmt{}, false},
		{"Commit", "COMMIT", query.CommitStmt{}, false},
		{"Rollback transaction", "COMMIT TRANSACTION", query.CommitStmt{}, false},
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
