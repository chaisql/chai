package parser_test

import (
	"testing"

	"github.com/chaisql/chai/internal/query"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/stretchr/testify/require"
)

func TestParserTransactions(t *testing.T) {
	tests := []struct {
		s        string
		expected statement.Statement
		errored  bool
	}{
		{"BEGIN", query.BeginStmt{Writable: true}, false},
		{"BEGIN TRANSACTION", query.BeginStmt{Writable: true}, false},
		{"BEGIN READ ONLY", query.BeginStmt{Writable: false}, false},
		{"BEGIN READ WRITE", query.BeginStmt{Writable: true}, false},
		{"BEGIN READ", query.BeginStmt{}, true},
		{"BEGIN WRITE", query.BeginStmt{}, true},
		{"ROLLBACK", query.RollbackStmt{}, false},
		{"ROLLBACK TRANSACTION", query.RollbackStmt{}, false},
		{"COMMIT", query.CommitStmt{}, false},
		{"COMMIT TRANSACTION", query.CommitStmt{}, false},
	}

	for _, test := range tests {
		t.Run(test.s, func(t *testing.T) {
			stmts, err := parser.ParseQuery(test.s)
			if test.errored {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			require.EqualValues(t, test.expected, stmts[0])
		})
	}
}
