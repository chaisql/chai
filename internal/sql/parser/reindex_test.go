package parser_test

import (
	"testing"

	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/stretchr/testify/require"
)

func TestParserReIndex(t *testing.T) {
	var r1 statement.ReIndexStmt
	var r2 statement.ReIndexStmt
	r2.TableOrIndexName = "tableOrIndex"
	tests := []struct {
		name     string
		s        string
		expected statement.Statement
		errored  bool
	}{
		{"All", "REINDEX", &r1, false},
		{"With ident", "REINDEX tableOrIndex", &r2, false},
		{"With extra", "REINDEX tableOrIndex tableOrIndex", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
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
