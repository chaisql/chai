package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/genjidb/genji/sql/planner"
	"github.com/genjidb/genji/sql/query"
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
			q, err := ParseQuery(context.Background(), test.s)
			require.NoError(t, err)
			require.EqualValues(t, test.expected, q.Statements)
		})
	}
}

func TestParserDivideByZero(t *testing.T) {
	// See https://github.com/genjidb/genji/issues/268
	require.NotPanics(t, func() {
		_, _ = ParseQuery(context.Background(), "SELECT*FROM t LIMIT-0%.2")
	})
}
