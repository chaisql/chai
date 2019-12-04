package parser

import (
	"testing"

	"github.com/asdine/genji/query"
	"github.com/stretchr/testify/require"
)

func TestParserMultiStatement(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected []query.Statement
	}{
		{"OnlyCommas", ";;;", nil},
		{"TrailingComma", "SELECT * FROM foo;;;DELETE FROM foo;", []query.Statement{
			query.SelectStmt{Selectors: []query.ResultField{query.Wildcard{}}, TableName: "foo"},
			query.DeleteStmt{TableName: "foo"},
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := ParseQuery(test.s)
			require.NoError(t, err)
			require.EqualValues(t, test.expected, q.Statements)
		})
	}
}
