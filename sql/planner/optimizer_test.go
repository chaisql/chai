package planner

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/stretchr/testify/require"
)

func TestSplitANDConditionRule(t *testing.T) {
	tests := []struct {
		name           string
		root, expected Node
	}{
		{
			"no and",
			NewSelectionNode(NewTableInputNode("foo"), expr.BoolValue(true)),
			NewSelectionNode(NewTableInputNode("foo"), expr.BoolValue(true)),
		},
		{
			"and / top-level selection node",
			NewSelectionNode(NewTableInputNode("foo"),
				expr.And(
					expr.BoolValue(true),
					expr.BoolValue(false),
				),
			),
			NewSelectionNode(
				NewSelectionNode(
					NewTableInputNode("foo"),
					expr.BoolValue(false)),
				expr.BoolValue(true)),
		},
		{
			"and / middle-level selection node",
			NewLimitNode(
				NewSelectionNode(NewTableInputNode("foo"),
					expr.And(
						expr.BoolValue(true),
						expr.BoolValue(false),
					),
				), expr.BoolValue(true)),
			NewLimitNode(
				NewSelectionNode(
					NewSelectionNode(
						NewTableInputNode("foo"),
						expr.BoolValue(false)),
					expr.BoolValue(true),
				), expr.BoolValue(true)),
		},
		{
			"multi and",
			NewLimitNode(
				NewSelectionNode(NewTableInputNode("foo"),
					expr.And(
						expr.And(
							expr.IntValue(1),
							expr.IntValue(2),
						),
						expr.And(
							expr.IntValue(3),
							expr.IntValue(4),
						),
					),
				), expr.IntValue(10)),
			NewLimitNode(
				NewSelectionNode(
					NewSelectionNode(
						NewSelectionNode(
							NewSelectionNode(
								NewTableInputNode("foo"),
								expr.IntValue(4)),
							expr.IntValue(3)),
						expr.IntValue(2)),
					expr.IntValue(1)),
				expr.IntValue(10),
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := splitANDConditionRule(NewTree(test.root))
			require.NoError(t, err)
			require.True(t, res.Root.Equal(test.expected))
		})
	}
}

func TestPrecalculateExprRule(t *testing.T) {
	tests := []struct {
		name        string
		e, expected expr.Expr
	}{
		{
			"constant expr: 3 -> 3",
			expr.IntValue(3),
			expr.IntValue(3),
		},
		{
			"operator with two constant operands: 3 + true -> 4",
			expr.Add(expr.IntValue(3), expr.BoolValue(true)),
			expr.IntValue(4),
		},
		{
			"operator with constant nested operands: 3 > true - 40 -> true",
			expr.Gt(expr.IntValue(3), expr.Sub(expr.BoolValue(true), expr.Float64Value(40))),
			expr.BoolValue(true),
		},
		{
			"constant sub-expr: a > true - 40 -> a > -39",
			expr.Gt(expr.FieldSelector{"a"}, expr.Sub(expr.BoolValue(true), expr.Float64Value(40))),
			expr.Gt(expr.FieldSelector{"a"}, expr.Float64Value(-39)),
		},
		{
			"non-constant expr list: [a, true - 40] -> [a, -39]",
			expr.LiteralExprList{
				expr.FieldSelector([]string{"a"}),
				expr.Sub(expr.BoolValue(true), expr.Float64Value(40)),
			},
			expr.LiteralExprList{
				expr.FieldSelector([]string{"a"}),
				expr.Float64Value(-39),
			},
		},
		{
			"constant expr list: [3, true - 40] -> array([3, 40])",
			expr.LiteralExprList{
				expr.IntValue(3),
				expr.Sub(expr.BoolValue(true), expr.Float64Value(40)),
			},
			expr.LiteralValue(document.NewArrayValue(document.NewValueBuffer().
				Append(document.NewIntValue(3)).
				Append(document.NewFloat64Value(-39)))),
		},
		{
			`non-constant kvpair: {"a": d, "b": 1 - 40} -> {"a": 3, "b": -39}`,
			expr.KVPairs{
				{K: "a", V: expr.FieldSelector{"d"}},
				{K: "b", V: expr.Sub(expr.BoolValue(true), expr.Float64Value(40))},
			},
			expr.KVPairs{
				{K: "a", V: expr.FieldSelector{"d"}},
				{K: "b", V: expr.Float64Value(-39)},
			},
		},
		{
			`constant kvpair: {"a": 3, "b": 1 - 40} -> document({"a": 3, "b": -39})`,
			expr.KVPairs{
				{K: "a", V: expr.IntValue(3)},
				{K: "b", V: expr.Sub(expr.BoolValue(true), expr.Float64Value(40))},
			},
			expr.LiteralValue(document.NewDocumentValue(document.NewFieldBuffer().
				Add("a", document.NewIntValue(3)).
				Add("b", document.NewFloat64Value(-39)),
			)),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := precalculateExprRule(NewTree(NewSelectionNode(NewTableInputNode("foo"), test.e)))
			require.NoError(t, err)
			require.True(t, res.Root.Equal(NewSelectionNode(NewTableInputNode("foo"), test.expected)))
		})
	}
}

func TestRemoveUnnecessarySelectionNodesRule(t *testing.T) {
	tests := []struct {
		name           string
		root, expected Node
	}{
		{
			"non-constant expr",
			NewSelectionNode(NewTableInputNode("foo"), expr.FieldSelector{"a"}),
			NewSelectionNode(NewTableInputNode("foo"), expr.FieldSelector{"a"}),
		},
		{
			"truthy constant expr",
			NewSelectionNode(NewTableInputNode("foo"), expr.IntValue(10)),
			NewTableInputNode("foo"),
		},
		{
			"falsy constant expr",
			NewSelectionNode(NewTableInputNode("foo"), expr.IntValue(0)),
			nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := removeUnnecessarySelectionNodesRule(NewTree(test.root))
			require.NoError(t, err)
			if test.expected != nil {
				require.True(t, test.expected.Equal(res.Root))
			} else {
				require.Equal(t, test.expected, res.Root)
			}

		})
	}
}
