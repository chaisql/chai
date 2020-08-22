package parser

import (
	"testing"

	"github.com/genjidb/genji/sql/planner"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/stretchr/testify/require"
)

func TestParserDelete(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected *planner.Tree
	}{
		{"NoCond", "DELETE FROM test",
			planner.NewTree(planner.NewDeletionNode(
				planner.NewTableInputNode("test"),
				"test"))},
		{"WithCond", "DELETE FROM test WHERE age = 10",
			planner.NewTree(planner.NewDeletionNode(
				planner.NewSelectionNode(
					planner.NewTableInputNode("test"),
					expr.Eq(expr.FieldSelector(newFieldRef(t, "age")), expr.IntegerValue(10))),
				"test"))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := ParseQuery(test.s)
			require.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}
