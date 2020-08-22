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
							expr.IntegerValue(1),
							expr.IntegerValue(2),
						),
						expr.And(
							expr.IntegerValue(3),
							expr.IntegerValue(4),
						),
					),
				), 10),
			planner.NewLimitNode(
				planner.NewSelectionNode(
					planner.NewSelectionNode(
						planner.NewSelectionNode(
							planner.NewSelectionNode(
								planner.NewTableInputNode("foo"),
								expr.IntegerValue(4)),
							expr.IntegerValue(3)),
						expr.IntegerValue(2)),
					expr.IntegerValue(1)),
				10,
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := planner.SplitANDConditionRule(planner.NewTree(test.root))
			require.NoError(t, err)
			require.Equal(t, res.String(), planner.NewTree(test.expected).String())
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
			expr.IntegerValue(3),
			expr.IntegerValue(3),
		},
		{
			"operator with two constant operands: 3 + 2.4 -> 5.4",
			expr.Add(expr.IntegerValue(3), expr.DoubleValue(2.4)),
			expr.DoubleValue(5.4),
		},
		{
			"operator with constant nested operands: 3 > 1 - 40 -> true",
			expr.Gt(expr.DoubleValue(3), expr.Sub(expr.IntegerValue(1), expr.DoubleValue(40))),
			expr.BoolValue(true),
		},
		{
			"constant sub-expr: a > 1 - 40 -> a > -39",
			expr.Gt(expr.FieldSelector{document.ValuePathFragment{FieldName: "a"}}, expr.Sub(expr.IntegerValue(1), expr.DoubleValue(40))),
			expr.Gt(expr.FieldSelector{document.ValuePathFragment{FieldName: "a"}}, expr.DoubleValue(-39)),
		},
		{
			"non-constant expr list: [a, 1 - 40] -> [a, -39]",
			expr.LiteralExprList{
				expr.FieldSelector{document.ValuePathFragment{FieldName: "a"}},
				expr.Sub(expr.IntegerValue(1), expr.DoubleValue(40)),
			},
			expr.LiteralExprList{
				expr.FieldSelector{document.ValuePathFragment{FieldName: "a"}},
				expr.DoubleValue(-39),
			},
		},
		{
			"constant expr list: [3, 1 - 40] -> array([3, -39])",
			expr.LiteralExprList{
				expr.IntegerValue(3),
				expr.Sub(expr.IntegerValue(1), expr.DoubleValue(40)),
			},
			expr.LiteralValue(document.NewArrayValue(document.NewValueBuffer().
				Append(document.NewIntegerValue(3)).
				Append(document.NewDoubleValue(-39)))),
		},
		{
			`non-constant kvpair: {"a": d, "b": 1 - 40} -> {"a": 3, "b": -39}`,
			expr.KVPairs{
				{K: "a", V: expr.FieldSelector{document.ValuePathFragment{FieldName: "d"}}},
				{K: "b", V: expr.Sub(expr.IntegerValue(1), expr.DoubleValue(40))},
			},
			expr.KVPairs{
				{K: "a", V: expr.FieldSelector{document.ValuePathFragment{FieldName: "d"}}},
				{K: "b", V: expr.DoubleValue(-39)},
			},
		},
		{
			`constant kvpair: {"a": 3, "b": 1 - 40} -> document({"a": 3, "b": -39})`,
			expr.KVPairs{
				{K: "a", V: expr.IntegerValue(3)},
				{K: "b", V: expr.Sub(expr.IntegerValue(1), expr.DoubleValue(40))},
			},
			expr.LiteralValue(document.NewDocumentValue(document.NewFieldBuffer().
				Add("a", document.NewIntegerValue(3)).
				Add("b", document.NewDoubleValue(-39)),
			)),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := planner.PrecalculateExprRule(planner.NewTree(planner.NewSelectionNode(planner.NewTableInputNode("foo"), test.e)))
			require.NoError(t, err)
			require.Equal(t, planner.NewTree(planner.NewSelectionNode(planner.NewTableInputNode("foo"), test.expected)).String(), res.String())
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
			planner.NewSelectionNode(planner.NewTableInputNode("foo"), expr.FieldSelector{document.ValuePathFragment{FieldName: "a"}}),
			planner.NewSelectionNode(planner.NewTableInputNode("foo"), expr.FieldSelector{document.ValuePathFragment{FieldName: "a"}}),
		},
		{
			"truthy constant expr",
			planner.NewSelectionNode(planner.NewTableInputNode("foo"), expr.IntegerValue(10)),
			planner.NewTableInputNode("foo"),
		},
		{
			"falsy constant expr",
			planner.NewSelectionNode(planner.NewTableInputNode("foo"), expr.IntegerValue(0)),
			nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := planner.RemoveUnnecessarySelectionNodesRule(planner.NewTree(test.root))
			require.NoError(t, err)
			if test.expected != nil {
				require.Equal(t, planner.NewTree(test.expected).String(), res.String())
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
					expr.FieldSelector{document.ValuePathFragment{FieldName: "d"}},
					expr.IntegerValue(1),
				)),
			nil,
		},
		{
			"FROM foo WHERE a = 1",
			planner.NewSelectionNode(planner.NewTableInputNode("foo"),
				expr.Eq(
					expr.FieldSelector{document.ValuePathFragment{FieldName: "a"}},
					expr.IntegerValue(1),
				)),
			planner.NewIndexInputNode(
				"foo",
				"idx_foo_a",
				expr.Eq(nil, nil).(planner.IndexIteratorOperator),
				expr.IntegerValue(1),
				scanner.ASC,
			),
		},
		{
			"FROM foo WHERE a = 1 AND b = 2",
			planner.NewSelectionNode(
				planner.NewSelectionNode(planner.NewTableInputNode("foo"),
					expr.Eq(
						expr.FieldSelector{document.ValuePathFragment{FieldName: "a"}},
						expr.IntegerValue(1),
					),
				),
				expr.Eq(
					expr.FieldSelector{document.ValuePathFragment{FieldName: "b"}},
					expr.IntegerValue(2),
				),
			),
			planner.NewSelectionNode(
				planner.NewIndexInputNode(
					"foo",
					"idx_foo_b",
					expr.Eq(nil, nil).(planner.IndexIteratorOperator),
					expr.IntegerValue(2),
					scanner.ASC,
				),
				expr.Eq(
					expr.FieldSelector{document.ValuePathFragment{FieldName: "a"}},
					expr.IntegerValue(1),
				),
			),
		},
		{
			"FROM foo WHERE c = 3 AND b = 2",
			planner.NewSelectionNode(
				planner.NewSelectionNode(planner.NewTableInputNode("foo"),
					expr.Eq(
						expr.FieldSelector{document.ValuePathFragment{FieldName: "c"}},
						expr.IntegerValue(3),
					),
				),
				expr.Eq(
					expr.FieldSelector{document.ValuePathFragment{FieldName: "b"}},
					expr.IntegerValue(2),
				),
			),
			planner.NewSelectionNode(
				planner.NewIndexInputNode(
					"foo",
					"idx_foo_c",
					expr.Eq(nil, nil).(planner.IndexIteratorOperator),
					expr.IntegerValue(3),
					scanner.ASC,
				),
				expr.Eq(
					expr.FieldSelector{document.ValuePathFragment{FieldName: "b"}},
					expr.IntegerValue(2),
				),
			),
		},
		{
			"SELECT a FROM foo WHERE c = 3 AND b = 2",
			planner.NewProjectionNode(
				planner.NewSelectionNode(
					planner.NewSelectionNode(planner.NewTableInputNode("foo"),
						expr.Eq(
							expr.FieldSelector{document.ValuePathFragment{FieldName: "c"}},
							expr.IntegerValue(3),
						),
					),
					expr.Eq(
						expr.FieldSelector{document.ValuePathFragment{FieldName: "b"}},
						expr.IntegerValue(2),
					),
				),
				[]planner.ResultField{
					planner.ResultFieldExpr{
						Expr: expr.FieldSelector{document.ValuePathFragment{FieldName: "a"}},
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
						expr.IntegerValue(3),
						scanner.ASC,
					),
					expr.Eq(
						expr.FieldSelector{document.ValuePathFragment{FieldName: "b"}},
						expr.IntegerValue(2),
					),
				),
				[]planner.ResultField{
					planner.ResultFieldExpr{
						Expr: expr.FieldSelector{document.ValuePathFragment{FieldName: "a"}},
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
				require.Equal(t, planner.NewTree(test.expected).String(), res.String())
			} else {
				require.Equal(t, res.Root, res.Root)
			}
		})
	}
}
