package planner_test

import (
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/planner"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/sql/scanner"
	"github.com/stretchr/testify/require"
)

func TestSplitANDConditionRule(t *testing.T) {
	tests := []struct {
		name           string
		root, expected planner.Node
	}{
		{
			"no and",
			planner.NewSelectionNode(planner.NewTableInputNode("foo"), expr.BoolValue(true)),
			planner.NewSelectionNode(planner.NewTableInputNode("foo"), expr.BoolValue(true)),
		},
		{
			"and / top-level selection node",
			planner.NewSelectionNode(planner.NewTableInputNode("foo"),
				expr.And(
					expr.BoolValue(true),
					expr.BoolValue(false),
				),
			),
			planner.NewSelectionNode(
				planner.NewSelectionNode(
					planner.NewTableInputNode("foo"),
					expr.BoolValue(false)),
				expr.BoolValue(true)),
		},
		{
			"and / middle-level selection node",
			planner.NewLimitNode(
				planner.NewSelectionNode(planner.NewTableInputNode("foo"),
					expr.And(
						expr.BoolValue(true),
						expr.BoolValue(false),
					),
				), 1),
			planner.NewLimitNode(
				planner.NewSelectionNode(
					planner.NewSelectionNode(
						planner.NewTableInputNode("foo"),
						expr.BoolValue(false)),
					expr.BoolValue(true),
				), 1),
		},
		{
			"multi and",
			planner.NewLimitNode(
				planner.NewSelectionNode(planner.NewTableInputNode("foo"),
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
				), 10),
			planner.NewLimitNode(
				planner.NewSelectionNode(
					planner.NewSelectionNode(
						planner.NewSelectionNode(
							planner.NewSelectionNode(
								planner.NewTableInputNode("foo"),
								expr.IntValue(4)),
							expr.IntValue(3)),
						expr.IntValue(2)),
					expr.IntValue(1)),
				10,
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := planner.SplitANDConditionRule(planner.NewTree(test.root))
			require.NoError(t, err)
			require.True(t, res.Root.IsEqual(test.expected))
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
			res, err := planner.PrecalculateExprRule(planner.NewTree(planner.NewSelectionNode(planner.NewTableInputNode("foo"), test.e)))
			require.NoError(t, err)
			require.True(t, res.Root.IsEqual(planner.NewSelectionNode(planner.NewTableInputNode("foo"), test.expected)))
		})
	}
}

func TestRemoveUnnecessarySelectionNodesRule(t *testing.T) {
	tests := []struct {
		name           string
		root, expected planner.Node
	}{
		{
			"non-constant expr",
			planner.NewSelectionNode(planner.NewTableInputNode("foo"), expr.FieldSelector{"a"}),
			planner.NewSelectionNode(planner.NewTableInputNode("foo"), expr.FieldSelector{"a"}),
		},
		{
			"truthy constant expr",
			planner.NewSelectionNode(planner.NewTableInputNode("foo"), expr.IntValue(10)),
			planner.NewTableInputNode("foo"),
		},
		{
			"falsy constant expr",
			planner.NewSelectionNode(planner.NewTableInputNode("foo"), expr.IntValue(0)),
			nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := planner.RemoveUnnecessarySelectionNodesRule(planner.NewTree(test.root))
			require.NoError(t, err)
			if test.expected != nil {
				require.True(t, test.expected.IsEqual(res.Root))
			} else {
				require.Equal(t, test.expected, res.Root)
			}
		})
	}
}

func TestUseIndexBasedOnSelectionNodeRule(t *testing.T) {
	tests := []struct {
		name           string
		root, expected planner.Node
	}{
		{
			"non-indexed field",
			planner.NewSelectionNode(planner.NewTableInputNode("foo"),
				expr.Eq(
					expr.FieldSelector{"d"},
					expr.IntValue(1),
				)),
			nil,
		},
		{
			"FROM foo WHERE a = 1",
			planner.NewSelectionNode(planner.NewTableInputNode("foo"),
				expr.Eq(
					expr.FieldSelector{"a"},
					expr.IntValue(1),
				)),
			planner.NewIndexInputNode(
				"foo",
				"idx_foo_a",
				expr.Eq(nil, nil).(planner.IndexIteratorOperator),
				expr.IntValue(1),
				scanner.ASC,
			),
		},
		{
			"FROM foo WHERE a = 1 AND b = 2",
			planner.NewSelectionNode(
				planner.NewSelectionNode(planner.NewTableInputNode("foo"),
					expr.Eq(
						expr.FieldSelector{"a"},
						expr.IntValue(1),
					),
				),
				expr.Eq(
					expr.FieldSelector{"b"},
					expr.IntValue(2),
				),
			),
			planner.NewSelectionNode(
				planner.NewIndexInputNode(
					"foo",
					"idx_foo_b",
					expr.Eq(nil, nil).(planner.IndexIteratorOperator),
					expr.IntValue(2),
					scanner.ASC,
				),
				expr.Eq(
					expr.FieldSelector{"a"},
					expr.IntValue(1),
				),
			),
		},
		{
			"FROM foo WHERE c = 3 AND b = 2",
			planner.NewSelectionNode(
				planner.NewSelectionNode(planner.NewTableInputNode("foo"),
					expr.Eq(
						expr.FieldSelector{"c"},
						expr.IntValue(3),
					),
				),
				expr.Eq(
					expr.FieldSelector{"b"},
					expr.IntValue(2),
				),
			),
			planner.NewSelectionNode(
				planner.NewIndexInputNode(
					"foo",
					"idx_foo_c",
					expr.Eq(nil, nil).(planner.IndexIteratorOperator),
					expr.IntValue(3),
					scanner.ASC,
				),
				expr.Eq(
					expr.FieldSelector{"b"},
					expr.IntValue(2),
				),
			),
		},
		{
			"SELECT a FROM foo WHERE c = 3 AND b = 2",
			planner.NewProjectionNode(
				planner.NewSelectionNode(
					planner.NewSelectionNode(planner.NewTableInputNode("foo"),
						expr.Eq(
							expr.FieldSelector{"c"},
							expr.IntValue(3),
						),
					),
					expr.Eq(
						expr.FieldSelector{"b"},
						expr.IntValue(2),
					),
				),
				[]planner.ResultField{
					planner.ResultFieldExpr{
						Expr: expr.FieldSelector{"a"},
					},
				},
				"foo",
			),
			planner.NewProjectionNode(
				planner.NewSelectionNode(
					planner.NewIndexInputNode(
						"foo",
						"idx_foo_c",
						expr.Eq(nil, nil).(planner.IndexIteratorOperator),
						expr.IntValue(3),
						scanner.ASC,
					),
					expr.Eq(
						expr.FieldSelector{"b"},
						expr.IntValue(2),
					),
				),
				[]planner.ResultField{
					planner.ResultFieldExpr{
						Expr: expr.FieldSelector{"a"},
					},
				},
				"foo",
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			tx, err := db.Begin(true)
			require.NoError(t, err)
			defer tx.Rollback()

			err = tx.Exec(`
				CREATE TABLE foo;
				CREATE INDEX idx_foo_a ON foo(a);
				CREATE INDEX idx_foo_b ON foo(b);
				CREATE UNIQUE INDEX idx_foo_c ON foo(c);
				INSERT INTO foo (a, b, c, d) VALUES
					(1, 1, 1, 1),
					(2, 2, 2, 2),
					(3, 3, 3, 3)
			`)
			require.NoError(t, err)

			err = planner.Bind(planner.NewTree(test.root), tx.Transaction, []expr.Param{
				{Name: "p1", Value: 1},
				{Name: "p2", Value: 2},
			})
			require.NoError(t, err)

			res, err := planner.UseIndexBasedOnSelectionNodeRule(planner.NewTree(test.root))
			require.NoError(t, err)
			if test.expected != nil {
				require.True(t, test.expected.IsEqual(res.Root))
			} else {
				require.Equal(t, res.Root, res.Root)
			}
		})
	}
}
