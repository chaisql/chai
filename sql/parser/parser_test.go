package parser

import (
	"testing"

	"github.com/genjidb/genji/sql/planner"
	"github.com/genjidb/genji/sql/query"
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
			planner.NewTree(
				planner.NewProjectionNode(
					planner.NewTableInputNode("foo"),
					[]planner.ProjectedField{
						planner.Wildcard{},
					},
					"foo",
				),
			),
			planner.NewTree(
				planner.NewDeletionNode(
					planner.NewTableInputNode("foo"),
					"foo",
				),
			),
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
