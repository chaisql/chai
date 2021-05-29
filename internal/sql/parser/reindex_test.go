package parser_test

import (
	"testing"

	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/stretchr/testify/require"
)

func TestParserReIndex(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected statement.Statement
		errored  bool
	}{
		{"All", "REINDEX", statement.ReIndexStmt{}, false},
		{"With ident", "REINDEX tableOrIndex", statement.ReIndexStmt{TableOrIndexName: "tableOrIndex"}, false},
		{"With extra", "REINDEX tableOrIndex tableOrIndex", nil, true},
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
