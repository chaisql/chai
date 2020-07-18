package parser

import (
	"testing"

	"github.com/genjidb/genji/sql/planner"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/sql/scanner"
	"github.com/stretchr/testify/require"
)

func TestParserSelect(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected *planner.Tree
		mustFail bool
	}{
		{"NoTable", "SELECT 1",
			planner.NewTree(planner.NewProjectionNode(nil,
				[]planner.ResultField{
					planner.ResultFieldExpr{Expr: expr.IntegerValue(1), ExprName: "1"},
				}, "")),
			false,
		},
		{"NoCond", "SELECT * FROM test",
			planner.NewTree(
				planner.NewProjectionNode(
					planner.NewTableInputNode("test"),
					[]planner.ResultField{planner.Wildcard{}},
					"test",
				)),
			false},
		{"WithFields", "SELECT a, b FROM test",
			planner.NewTree(
				planner.NewProjectionNode(
					planner.NewTableInputNode("test"),
					[]planner.ResultField{planner.ResultFieldExpr{Expr: expr.FieldSelector([]string{"a"}), ExprName: "a"}, planner.ResultFieldExpr{Expr: expr.FieldSelector([]string{"b"}), ExprName: "b"}},
					"test",
				)),
			false},
		{"WithFieldsWithQuotes", "SELECT `long \"field\"` FROM test",
			planner.NewTree(
				planner.NewProjectionNode(
					planner.NewTableInputNode("test"),
					[]planner.ResultField{planner.ResultFieldExpr{Expr: expr.FieldSelector([]string{"long \"field\""}), ExprName: "long \"field\""}},
					"test",
				)),
			false},
		{"WithAlias", "SELECT a AS A, b FROM test",
			planner.NewTree(
				planner.NewProjectionNode(
					planner.NewTableInputNode("test"),
					[]planner.ResultField{planner.ResultFieldExpr{Expr: expr.FieldSelector([]string{"a"}), ExprName: "A"}, planner.ResultFieldExpr{Expr: expr.FieldSelector([]string{"b"}), ExprName: "b"}},
					"test",
				)),
			false},
		{"WithFields and wildcard", "SELECT a, b, * FROM test",
			planner.NewTree(
				planner.NewProjectionNode(
					planner.NewTableInputNode("test"),
					[]planner.ResultField{planner.ResultFieldExpr{Expr: expr.FieldSelector([]string{"a"}), ExprName: "a"}, planner.ResultFieldExpr{Expr: expr.FieldSelector([]string{"b"}), ExprName: "b"}, planner.Wildcard{}},
					"test",
				)),
			false},
		{"WithExpr", "SELECT a    > 1 FROM test",
			planner.NewTree(
				planner.NewProjectionNode(
					planner.NewTableInputNode("test"),
					[]planner.ResultField{planner.ResultFieldExpr{Expr: expr.Gt(expr.FieldSelector([]string{"a"}), expr.IntegerValue(1)), ExprName: "a    > 1"}},
					"test",
				)),
			false},
		{"WithCond", "SELECT * FROM test WHERE age = 10",
			planner.NewTree(
				planner.NewProjectionNode(
					planner.NewSelectionNode(
						planner.NewTableInputNode("test"),
						expr.Eq(expr.FieldSelector([]string{"age"}), expr.IntegerValue(10)),
					),
					[]planner.ResultField{planner.Wildcard{}},
					"test",
				)),
			false},
		{"WithOrderBy", "SELECT * FROM test WHERE age = 10 ORDER BY a.b.c",
			planner.NewTree(
				planner.NewProjectionNode(
					planner.NewSortNode(
						planner.NewSelectionNode(
							planner.NewTableInputNode("test"),
							expr.Eq(expr.FieldSelector([]string{"age"}), expr.IntegerValue(10)),
						),
						[]string{"a", "b", "c"},
						scanner.ASC,
					),
					[]planner.ResultField{planner.Wildcard{}},
					"test",
				)),
			false},
		{"WithOrderBy ASC", "SELECT * FROM test WHERE age = 10 ORDER BY a.b.c ASC",
			planner.NewTree(
				planner.NewProjectionNode(
					planner.NewSortNode(
						planner.NewSelectionNode(
							planner.NewTableInputNode("test"),
							expr.Eq(expr.FieldSelector([]string{"age"}), expr.IntegerValue(10)),
						),
						[]string{"a", "b", "c"},
						scanner.ASC,
					),
					[]planner.ResultField{planner.Wildcard{}},
					"test",
				)),
			false},
		{"WithOrderBy DESC", "SELECT * FROM test WHERE age = 10 ORDER BY a.b.c DESC",
			planner.NewTree(
				planner.NewProjectionNode(
					planner.NewSortNode(
						planner.NewSelectionNode(
							planner.NewTableInputNode("test"),
							expr.Eq(expr.FieldSelector([]string{"age"}), expr.IntegerValue(10)),
						),
						[]string{"a", "b", "c"},
						scanner.DESC,
					),
					[]planner.ResultField{planner.Wildcard{}},
					"test",
				)),
			false},
		{"WithLimit", "SELECT * FROM test WHERE age = 10 LIMIT 20",
			planner.NewTree(
				planner.NewProjectionNode(
					planner.NewLimitNode(
						planner.NewSelectionNode(
							planner.NewTableInputNode("test"),
							expr.Eq(expr.FieldSelector([]string{"age"}), expr.IntegerValue(10)),
						),
						20,
					),
					[]planner.ResultField{planner.Wildcard{}},
					"test",
				)),
			false},
		{"WithOffset", "SELECT * FROM test WHERE age = 10 OFFSET 20",
			planner.NewTree(
				planner.NewProjectionNode(
					planner.NewOffsetNode(
						planner.NewSelectionNode(
							planner.NewTableInputNode("test"),
							expr.Eq(expr.FieldSelector([]string{"age"}), expr.IntegerValue(10)),
						),
						20,
					),
					[]planner.ResultField{planner.Wildcard{}},
					"test",
				)),
			false},
		{"WithLimitThenOffset", "SELECT * FROM test WHERE age = 10 LIMIT 10 OFFSET 20",
			planner.NewTree(
				planner.NewProjectionNode(
					planner.NewLimitNode(
						planner.NewOffsetNode(
							planner.NewSelectionNode(
								planner.NewTableInputNode("test"),
								expr.Eq(expr.FieldSelector([]string{"age"}), expr.IntegerValue(10)),
							),
							20,
						),
						10,
					),
					[]planner.ResultField{planner.Wildcard{}},
					"test",
				)),
			false},
		{"WithOffsetThenLimit", "SELECT * FROM test WHERE age = 10 OFFSET 20 LIMIT 10", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := ParseQuery(test.s)
			if !test.mustFail {
				require.NoError(t, err)
				require.Len(t, q.Statements, 1)
				require.EqualValues(t, test.expected, q.Statements[0])
			} else {
				require.Error(t, err)
			}
		})
	}
}
