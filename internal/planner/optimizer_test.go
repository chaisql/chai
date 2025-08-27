package planner_test

import (
	"testing"

	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/planner"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/index"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/stream/table"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestSplitANDConditionRule(t *testing.T) {
	tests := []struct {
		name         string
		in, expected *stream.Stream
	}{
		{
			"no and",
			stream.New(table.Scan("foo")).Pipe(rows.Filter(testutil.BoolValue(true))),
			stream.New(table.Scan("foo")).Pipe(rows.Filter(testutil.BoolValue(true))),
		},
		{
			"and / top-level selection node",
			stream.New(table.Scan("foo")).Pipe(rows.Filter(
				expr.And(
					testutil.BoolValue(true),
					testutil.BoolValue(false),
				),
			)),
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(testutil.BoolValue(true))).
				Pipe(rows.Filter(testutil.BoolValue(false))),
		},
		{
			"and / middle-level selection node",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(
					expr.And(
						testutil.BoolValue(true),
						testutil.BoolValue(false),
					),
				)).
				Pipe(rows.Take(parser.MustParseExpr("1"))),
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(testutil.BoolValue(true))).
				Pipe(rows.Filter(testutil.BoolValue(false))).
				Pipe(rows.Take(parser.MustParseExpr("1"))),
		},
		{
			"multi and",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(
					expr.And(
						expr.And(
							testutil.IntegerValue(1),
							testutil.IntegerValue(2),
						),
						expr.And(
							testutil.IntegerValue(3),
							testutil.IntegerValue(4),
						),
					),
				)).
				Pipe(rows.Take(parser.MustParseExpr("10"))),
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(testutil.IntegerValue(1))).
				Pipe(rows.Filter(testutil.IntegerValue(2))).
				Pipe(rows.Filter(testutil.IntegerValue(3))).
				Pipe(rows.Filter(testutil.IntegerValue(4))).
				Pipe(rows.Take(parser.MustParseExpr("10"))),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sctx := planner.NewStreamContext(test.in, nil)
			err := planner.SplitANDConditionRule(sctx)
			require.NoError(t, err)
			require.Equal(t, test.expected.String(), sctx.Stream.String())
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
			testutil.IntegerValue(3),
			testutil.IntegerValue(3),
		},
		{
			"operator with two constant operands: 3 + 2.4 -> 5.4",
			expr.Add(testutil.IntegerValue(3), testutil.DoubleValue(2.4)),
			testutil.DoubleValue(5.4),
		},
		{
			"operator with constant nested operands: 3 > 1 - 40 -> true",
			expr.Gt(testutil.DoubleValue(3), expr.Sub(testutil.IntegerValue(1), testutil.DoubleValue(40))),
			testutil.BoolValue(true),
		},
		{
			"constant sub-expr: a > 1 - 40 -> a > -39",
			expr.Gt(&expr.Column{Name: "a"}, expr.Sub(testutil.IntegerValue(1), testutil.DoubleValue(40))),
			expr.Gt(&expr.Column{Name: "a"}, testutil.DoubleValue(-39)),
		},
		{
			"non-constant expr list: (a, 1 - 40) -> (a, -39)",
			expr.LiteralExprList{
				&expr.Column{Name: "a"},
				expr.Sub(testutil.IntegerValue(1), testutil.DoubleValue(40)),
			},
			expr.LiteralExprList{
				&expr.Column{Name: "a"},
				testutil.DoubleValue(-39),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, `
				CREATE TABLE foo (k INT PRIMARY KEY, a INT);
			`)

			s := stream.New(table.Scan("foo")).
				Pipe(rows.Filter(test.e))

			sctx := planner.NewStreamContext(s, tx.Catalog)
			err := planner.PrecalculateExprRule(sctx)
			require.NoError(t, err)
			require.Equal(t, stream.New(table.Scan("foo")).Pipe(rows.Filter(test.expected)).String(), sctx.Stream.String())
		})
	}
}

func TestRemoveUnnecessarySelectionNodesRule(t *testing.T) {
	tests := []struct {
		name           string
		root, expected *stream.Stream
	}{
		{
			"non-constant expr",
			stream.New(table.Scan("foo")).Pipe(rows.Filter(parser.MustParseExpr("a"))),
			stream.New(table.Scan("foo")).Pipe(rows.Filter(parser.MustParseExpr("a"))),
		},
		{
			"truthy constant expr",
			stream.New(table.Scan("foo")).Pipe(rows.Filter(parser.MustParseExpr("10"))),
			stream.New(table.Scan("foo")),
		},
		{
			"falsy constant expr",
			stream.New(table.Scan("foo")).Pipe(rows.Filter(parser.MustParseExpr("0"))),
			&stream.Stream{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sctx := planner.NewStreamContext(test.root, nil)
			err := planner.RemoveUnnecessaryFilterNodesRule(sctx)
			require.NoError(t, err)
			require.Equal(t, test.expected.String(), sctx.Stream.String())
		})
	}
}

func exprList(list ...expr.Expr) expr.LiteralExprList {
	return expr.LiteralExprList(list)
}

func TestSelectIndex_Simple(t *testing.T) {
	tests := []struct {
		name           string
		root, expected *stream.Stream
	}{
		{
			"non-indexed path",
			stream.New(table.Scan("foo")).Pipe(rows.Filter(parser.MustParseExpr("d = 1"))),
			stream.New(table.Scan("foo")).Pipe(rows.Filter(parser.MustParseExpr("d = 1"))),
		},
		{
			"FROM foo WHERE a = 1",
			stream.New(table.Scan("foo")).Pipe(rows.Filter(parser.MustParseExpr("a = 1"))),
			stream.New(index.Scan("idx_foo_a", stream.Range{Min: exprList(testutil.IntegerValue(1)), Exact: true})),
		},
		{
			"FROM foo WHERE a = 1 AND b = 2",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(rows.Filter(parser.MustParseExpr("b = 2"))),
			stream.New(index.Scan("idx_foo_a", stream.Range{Min: exprList(testutil.IntegerValue(1)), Exact: true})).
				Pipe(rows.Filter(parser.MustParseExpr("b = 2"))),
		},
		{
			"FROM foo WHERE c = 3 AND b = 2",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("c = 3"))).
				Pipe(rows.Filter(parser.MustParseExpr("b = 2"))),
			stream.New(index.Scan("idx_foo_c", stream.Range{Min: exprList(testutil.IntegerValue(3)), Exact: true})).
				Pipe(rows.Filter(parser.MustParseExpr("b = 2"))),
		},
		{
			"FROM foo WHERE c > 3 AND b = 2",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("c > 3"))).
				Pipe(rows.Filter(parser.MustParseExpr("b = 2"))),
			stream.New(index.Scan("idx_foo_b", stream.Range{Min: exprList(testutil.IntegerValue(2)), Exact: true})).
				Pipe(rows.Filter(parser.MustParseExpr("c > 3"))),
		},
		{
			"SELECT a FROM foo WHERE c = 3 AND b = 2",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("c = 3"))).
				Pipe(rows.Filter(parser.MustParseExpr("b = 2"))).
				Pipe(rows.Project(parser.MustParseExpr("a"))),
			stream.New(index.Scan("idx_foo_c", stream.Range{Min: exprList(testutil.IntegerValue(3)), Exact: true})).
				Pipe(rows.Filter(parser.MustParseExpr("b = 2"))).
				Pipe(rows.Project(parser.MustParseExpr("a"))),
		},
		{
			"SELECT a FROM foo WHERE c = 3 AND d = 2",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("c = 3"))).
				Pipe(rows.Filter(parser.MustParseExpr("d = 2"))).
				Pipe(rows.Project(parser.MustParseExpr("a"))),
			stream.New(index.Scan("idx_foo_c", stream.Range{Min: exprList(testutil.IntegerValue(3)), Exact: true})).
				Pipe(rows.Filter(parser.MustParseExpr("d = 2"))).
				Pipe(rows.Project(parser.MustParseExpr("a"))),
		},
		{
			"FROM foo WHERE a IN (1, 2)",
			stream.New(table.Scan("foo")).Pipe(rows.Filter(
				expr.In(
					parser.MustParseExpr("a"),
					testutil.ExprList(t, `(1, 2)`),
				),
			)),
			stream.New(index.Scan("idx_foo_a", stream.Range{Min: exprList(testutil.IntegerValue(1)), Exact: true}, stream.Range{Min: exprList(testutil.IntegerValue(2)), Exact: true})),
		},
		{
			"FROM foo WHERE 1 IN a",
			stream.New(table.Scan("foo")).Pipe(rows.Filter(parser.MustParseExpr("1 IN a"))),
			stream.New(table.Scan("foo")).Pipe(rows.Filter(parser.MustParseExpr("1 IN a"))),
		},
		{
			"FROM foo WHERE a >= 10",
			stream.New(table.Scan("foo")).Pipe(rows.Filter(parser.MustParseExpr("a >= 10"))),
			stream.New(index.Scan("idx_foo_a", stream.Range{Min: exprList(testutil.IntegerValue(10))})),
		},
		{
			"FROM foo WHERE k = 1",
			stream.New(table.Scan("foo")).Pipe(rows.Filter(parser.MustParseExpr("k = 1"))),
			stream.New(table.Scan("foo", stream.Range{Min: exprList(testutil.IntegerValue(1)), Exact: true})),
		},
		{
			"FROM foo WHERE k = 1 AND b = 2",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("k = 1"))).
				Pipe(rows.Filter(parser.MustParseExpr("b = 2"))),
			stream.New(table.Scan("foo", stream.Range{Min: exprList(testutil.IntegerValue(1)), Exact: true})).
				Pipe(rows.Filter(parser.MustParseExpr("b = 2"))),
		},
		{
			"FROM foo WHERE a = 1 AND k = 2",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(rows.Filter(parser.MustParseExpr("2 = k"))),
			stream.New(table.Scan("foo", stream.Range{Min: exprList(testutil.IntegerValue(2)), Exact: true})).
				Pipe(rows.Filter(parser.MustParseExpr("a = 1"))),
		},
		{
			"FROM foo WHERE a = 1 AND k < 2",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(rows.Filter(parser.MustParseExpr("k < 2"))),
			stream.New(index.Scan("idx_foo_a", stream.Range{Min: exprList(testutil.IntegerValue(1)), Exact: true})).
				Pipe(rows.Filter(parser.MustParseExpr("k < 2"))),
		},
		{ // c is an INT, 1.1 cannot be converted to int without precision loss, don't use the index
			"FROM foo WHERE c < 1.1",
			stream.New(table.Scan("foo")).Pipe(rows.Filter(parser.MustParseExpr("c < 1.1"))),
			stream.New(table.Scan("foo")).Pipe(rows.Filter(parser.MustParseExpr("c < 1.1"))),
		},
		// {
		// 	"FROM foo WHERE a = 1 OR b = 2",
		// 	stream.New(table.TableScan("foo")).
		// 		Pipe(stream.Filter(parser.MustParseExpr("a = 1 OR b = 2"))),
		// 	stream.New(
		// 		stream.Union(
		// 			index.IndexScan("idx_foo_a", stream.IndexRange{Min: exprList(testutil.IntegerValue(1)), Exact: true}),
		// 			index.IndexScan("idx_foo_b", stream.IndexRange{Min: exprList(testutil.IntegerValue(2)), Exact: true}),
		// 		),
		// 	),
		// },
		// {
		// 	"FROM foo WHERE a = 1 OR b > 2",
		// 	stream.New(table.TableScan("foo")).
		// 		Pipe(stream.Filter(parser.MustParseExpr("a = 1 OR b = 2"))),
		// 	stream.New(
		// 		stream.Union(
		// 			index.IndexScan("idx_foo_a", stream.IndexRange{Min: exprList(testutil.IntegerValue(1)), Exact: true}),
		// 			index.IndexScan("idx_foo_b", stream.IndexRange{Min: exprList(testutil.IntegerValue(2)), Exclusive: true}),
		// 		),
		// 	),
		// },
		// {
		// 	"FROM foo WHERE a > 1 OR b > 2",
		// 	stream.New(table.TableScan("foo")).
		// 		Pipe(stream.Filter(parser.MustParseExpr("a = 1 OR b = 2"))),
		// 	stream.New(table.TableScan("foo")).
		// 		Pipe(stream.Filter(parser.MustParseExpr("a = 1 OR b = 2"))),
		// },
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, `
				CREATE TABLE foo (k INT PRIMARY KEY, a INT, b INT, c INT, d INT);
				CREATE INDEX idx_foo_a ON foo(a);
				CREATE INDEX idx_foo_b ON foo(b);
				CREATE UNIQUE INDEX idx_foo_c ON foo(c);
				INSERT INTO foo (k, a, b, c, d) VALUES
					(1, 1, 1, 1, 1),
					(2, 2, 2, 2, 2),
					(3, 3, 3, 3, 3)
			`)

			sctx := planner.NewStreamContext(test.root, tx.Catalog)
			sctx.Catalog = tx.Catalog
			st, err := planner.Optimize(test.root, tx.Catalog, nil)
			// err := planner.SelectIndex(sctx)
			require.NoError(t, err)
			require.Equal(t, test.expected.String(), st.String())
		})
	}
}

func TestSelectIndex_Composite(t *testing.T) {
	tests := []struct {
		name           string
		root, expected *stream.Stream
	}{
		{
			"FROM foo WHERE a = 1 AND d = 2",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(rows.Filter(parser.MustParseExpr("d = 2"))),
			stream.New(index.Scan("idx_foo_a_d", stream.Range{Min: testutil.ExprList(t, `(1, 2)`), Exact: true})),
		},
		{
			"FROM foo WHERE a = 1 AND d > 2",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(rows.Filter(parser.MustParseExpr("d > 2"))),
			stream.New(index.Scan("idx_foo_a_d", stream.Range{Min: testutil.ExprList(t, `(1, 2)`), Exclusive: true})),
		},
		{
			"FROM foo WHERE a = 1 AND d < 2",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(rows.Filter(parser.MustParseExpr("d < 2"))),
			stream.New(index.Scan("idx_foo_a_d", stream.Range{Max: testutil.ExprList(t, `(1, 2)`), Exclusive: true})),
		},
		{
			"FROM foo WHERE a = 1 AND d <= 2",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(rows.Filter(parser.MustParseExpr("d <= 2"))),
			stream.New(index.Scan("idx_foo_a_d", stream.Range{Max: testutil.ExprList(t, `(1, 2)`)})),
		},
		{
			"FROM foo WHERE a = 1 AND d >= 2",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(rows.Filter(parser.MustParseExpr("d >= 2"))),
			stream.New(index.Scan("idx_foo_a_d", stream.Range{Min: testutil.ExprList(t, `(1, 2)`)})),
		},
		{
			"FROM foo WHERE a > 1 AND d > 2",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("a > 1"))).
				Pipe(rows.Filter(parser.MustParseExpr("d > 2"))),
			stream.New(index.Scan("idx_foo_a", stream.Range{Min: testutil.ExprList(t, `(1)`), Exclusive: true})).
				Pipe(rows.Filter(parser.MustParseExpr("d > 2"))),
		},
		{
			"FROM foo WHERE a > $1 AND d > $2",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("a > $1"))).
				Pipe(rows.Filter(parser.MustParseExpr("d > $2"))),
			stream.New(index.Scan("idx_foo_a", stream.Range{Min: testutil.ExprList(t, `(1)`), Exclusive: true})).
				Pipe(rows.Filter(parser.MustParseExpr("d > 2"))),
		},
		{
			"FROM foo WHERE a = 1 AND b = 2 AND c = 3",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(rows.Filter(parser.MustParseExpr("b = 2"))).
				Pipe(rows.Filter(parser.MustParseExpr("c = 3"))),
			stream.New(index.Scan("idx_foo_a_b_c", stream.Range{Min: testutil.ExprList(t, `(1, 2, 3)`), Exact: true})),
		},
		{
			"FROM foo WHERE a = 1 AND b = 2", // c is omitted, but it can still use idx_foo_a_b_c
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(rows.Filter(parser.MustParseExpr("b = 2"))),
			stream.New(index.Scan("idx_foo_a_b_c", stream.Range{Min: testutil.ExprList(t, `(1, 2)`), Exact: true})),
		},
		{
			"FROM foo WHERE a = 1 AND b > 2", // c is omitted, but it can still use idx_foo_a_b_c, with > b
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(rows.Filter(parser.MustParseExpr("b > 2"))),
			stream.New(index.Scan("idx_foo_a_b_c", stream.Range{Min: testutil.ExprList(t, `(1, 2)`), Exclusive: true})),
		},
		{
			"FROM foo WHERE a = 1 AND b < 2", // c is omitted, but it can still use idx_foo_a_b_c, with > b
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(rows.Filter(parser.MustParseExpr("b < 2"))),
			stream.New(index.Scan("idx_foo_a_b_c", stream.Range{Max: testutil.ExprList(t, `(1, 2)`), Exclusive: true})),
		},
		{
			"FROM foo WHERE a = 1 AND b = 2 and k = 3", // c is omitted, but it can still use idx_foo_a_b_c
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(rows.Filter(parser.MustParseExpr("b = 2"))).
				Pipe(rows.Filter(parser.MustParseExpr("k = 3"))),
			stream.New(index.Scan("idx_foo_a_b_c", stream.Range{Min: testutil.ExprList(t, `(1, 2)`), Exact: true})).
				Pipe(rows.Filter(parser.MustParseExpr("k = 3"))),
		},
		// If a path is missing from the query, we can still the index, with paths after the missing one are
		// using filter nodes rather than the index.
		{
			"FROM foo WHERE x = 1 AND z = 2",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("x = 1"))).
				Pipe(rows.Filter(parser.MustParseExpr("z = 2"))),
			stream.New(index.Scan("idx_foo_x_y_z", stream.Range{Min: exprList(testutil.IntegerValue(1)), Exact: true})).
				Pipe(rows.Filter(parser.MustParseExpr("z = 2"))),
		},
		{
			"FROM foo WHERE a = 1 AND c = 2",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(rows.Filter(parser.MustParseExpr("c = 2"))),
			// c will be picked because it's a unique index and thus has a lower cost
			stream.New(index.Scan("idx_foo_c", stream.Range{Min: exprList(testutil.IntegerValue(2)), Exact: true})).
				Pipe(rows.Filter(parser.MustParseExpr("a = 1"))),
		},
		{
			"FROM foo WHERE b = 1 AND c = 2",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("b = 1"))).
				Pipe(rows.Filter(parser.MustParseExpr("c = 2"))),
			// c will be picked because it's a unique index and thus has a lower cost
			stream.New(index.Scan("idx_foo_c", stream.Range{Min: exprList(testutil.IntegerValue(2)), Exact: true})).
				Pipe(rows.Filter(parser.MustParseExpr("b = 1"))),
		},
		{
			"FROM foo WHERE a IN (1, 2) AND d = 4",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(
					expr.In(
						parser.MustParseExpr("a"),
						testutil.ExprList(t, `(1, 2)`),
					),
				)).
				Pipe(rows.Filter(parser.MustParseExpr("d = 4"))),
			stream.New(index.Scan("idx_foo_a_d",
				stream.Range{Min: testutil.ExprList(t, `(1, 4)`), Exact: true},
				stream.Range{Min: testutil.ExprList(t, `(2, 4)`), Exact: true},
			)),
		},
		{
			"FROM foo WHERE a IN (1, 2) AND b = 3 AND c = 4",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(
					expr.In(
						parser.MustParseExpr("a"),
						testutil.ExprList(t, `(1, 2)`),
					),
				)).
				Pipe(rows.Filter(parser.MustParseExpr("b = 3"))).
				Pipe(rows.Filter(parser.MustParseExpr("c = 4"))),
			stream.New(index.Scan("idx_foo_a_b_c",
				stream.Range{Min: testutil.ExprList(t, `(1, 3, 4)`), Exact: true},
				stream.Range{Min: testutil.ExprList(t, `(2, 3, 4)`), Exact: true},
			)),
		},
		{
			"FROM foo WHERE a IN (1, 2) AND b = 3 AND c > 4",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(
					expr.In(
						parser.MustParseExpr("a"),
						testutil.ExprList(t, `(1, 2)`),
					),
				)).
				Pipe(rows.Filter(parser.MustParseExpr("b = 3"))).
				Pipe(rows.Filter(parser.MustParseExpr("c > 4"))),
			stream.New(index.Scan("idx_foo_a_b_c",
				stream.Range{Min: testutil.ExprList(t, `(1, 3)`), Exact: true},
				stream.Range{Min: testutil.ExprList(t, `(2, 3)`), Exact: true},
			)).Pipe(rows.Filter(parser.MustParseExpr("c > 4"))),
		},
		{
			"FROM foo WHERE a IN (1, 2) AND b = 3 AND c < 4",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(
					expr.In(
						parser.MustParseExpr("a"),
						testutil.ExprList(t, `(1, 2)`),
					),
				)).
				Pipe(rows.Filter(parser.MustParseExpr("b = 3"))).
				Pipe(rows.Filter(parser.MustParseExpr("c < 4"))),
			stream.New(index.Scan("idx_foo_a_b_c",
				stream.Range{Min: testutil.ExprList(t, `(1, 3)`), Exact: true},
				stream.Range{Min: testutil.ExprList(t, `(2, 3)`), Exact: true},
			)).Pipe(rows.Filter(parser.MustParseExpr("c < 4"))),
		},
		{
			"FROM foo WHERE a IN (1, 2) AND b IN (3, 4) AND c > 5",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(
					expr.In(
						parser.MustParseExpr("a"),
						testutil.ExprList(t, `(1, 2)`),
					),
				)).
				Pipe(rows.Filter(
					expr.In(
						parser.MustParseExpr("b"),
						testutil.ExprList(t, `(3, 4)`),
					),
				)).
				Pipe(rows.Filter(parser.MustParseExpr("c > 5"))),
			stream.New(index.Scan("idx_foo_a_b_c",
				stream.Range{Min: testutil.ExprList(t, `(1, 3)`), Exact: true},
				stream.Range{Min: testutil.ExprList(t, `(1, 4)`), Exact: true},
				stream.Range{Min: testutil.ExprList(t, `(2, 3)`), Exact: true},
				stream.Range{Min: testutil.ExprList(t, `(2, 4)`), Exact: true},
			)).Pipe(rows.Filter(parser.MustParseExpr("c > 5"))),
		},
		{
			"FROM foo WHERE 1 IN a AND d = 2",
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("1 IN a"))).
				Pipe(rows.Filter(parser.MustParseExpr("d = 4"))),
			stream.New(table.Scan("foo")).
				Pipe(rows.Filter(parser.MustParseExpr("1 IN a"))).
				Pipe(rows.Filter(parser.MustParseExpr("d = 4"))),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, `
				CREATE TABLE foo (k INT PRIMARY KEY, a INT, b INT, c INT, d INT, x INT, y INT, z INT);
				CREATE INDEX idx_foo_a ON foo(a);
				CREATE INDEX idx_foo_b ON foo(b);
				CREATE UNIQUE INDEX idx_foo_c ON foo(c);
				CREATE INDEX idx_foo_a_d ON foo(a, d);
				CREATE INDEX idx_foo_a_b_c ON foo(a, b, c);
				CREATE INDEX idx_foo_x_y_z ON foo(x, y, z);
				INSERT INTO foo (k, a, b, c, d) VALUES
					(1, 1, 1, 1, 1),
					(2, 2, 2, 2, 2),
					(3, 3, 3, 3, 3)
			`)

			sctx := planner.NewStreamContext(test.root, tx.Catalog)
			sctx.Catalog = tx.Catalog
			st, err := planner.Optimize(test.root, tx.Catalog, []environment.Param{
				{Value: 1},
				{Value: 2},
			})
			require.NoError(t, err)
			require.Equal(t, test.expected.String(), st.String())
		})
	}
}

func TestOptimize(t *testing.T) {
	t.Run("concat and union operator operands are optimized", func(t *testing.T) {
		t.Run("PrecalculateExprRule", func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()
			testutil.MustExec(t, db, tx, `
				CREATE TABLE foo(a INT, b INT, c INT, d INT);
				CREATE TABLE bar(a INT, b INT, c INT, d INT);
			`)

			got, err := planner.Optimize(
				stream.New(stream.Union(
					stream.New(stream.Concat(
						stream.New(table.Scan("foo")).Pipe(rows.Filter(parser.MustParseExpr("a = 1 + 2"))),
						stream.New(table.Scan("bar")).Pipe(rows.Filter(parser.MustParseExpr("b = 1 + $1"))),
					)),
					stream.New(table.Scan("foo")).Pipe(rows.Filter(parser.MustParseExpr("c = 1 + 2"))),
					stream.New(table.Scan("bar")).Pipe(rows.Filter(parser.MustParseExpr("d = 1 + $2"))),
				)),
				tx.Catalog, []environment.Param{
					{Name: "1", Value: 2},
					{Name: "2", Value: 3},
				})
			require.NoError(t, err)

			want := stream.New(stream.Union(
				stream.New(stream.Concat(
					stream.New(table.Scan("foo")).Pipe(rows.Filter(parser.MustParseExpr("a = 3"))),
					stream.New(table.Scan("bar")).Pipe(rows.Filter(parser.MustParseExpr("b = 3"))),
				)),
				stream.New(table.Scan("foo")).Pipe(rows.Filter(parser.MustParseExpr("c = 3"))),
				stream.New(table.Scan("bar")).Pipe(rows.Filter(parser.MustParseExpr("d = 4"))),
			))

			require.Equal(t, want.String(), got.String())
		})

		t.Run("RemoveUnnecessarySelectionNodesRule", func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()
			testutil.MustExec(t, db, tx, `
				CREATE TABLE foo(a INT, b INT, c INT, d INT);
				CREATE TABLE bar(a INT, b INT, c INT, d INT);
			`)

			got, err := planner.Optimize(
				stream.New(stream.Union(
					stream.New(stream.Concat(
						stream.New(table.Scan("foo")).Pipe(rows.Filter(parser.MustParseExpr("10"))),
						stream.New(table.Scan("bar")).Pipe(rows.Filter(parser.MustParseExpr("11"))),
					)),
					stream.New(table.Scan("foo")).Pipe(rows.Filter(parser.MustParseExpr("12"))),
					stream.New(table.Scan("bar")).Pipe(rows.Filter(parser.MustParseExpr("13"))),
				)),
				tx.Catalog, nil)

			want := stream.New(stream.Union(
				stream.New(stream.Concat(
					stream.New(table.Scan("foo")),
					stream.New(table.Scan("bar")),
				)),
				stream.New(table.Scan("foo")),
				stream.New(table.Scan("bar")),
			))

			require.NoError(t, err)
			require.Equal(t, want.String(), got.String())
		})
	})

	t.Run("SelectIndex", func(t *testing.T) {
		db, tx, cleanup := testutil.NewTestTx(t)
		defer cleanup()
		testutil.MustExec(t, db, tx, `
				CREATE TABLE foo(a INT, d INT);
				CREATE TABLE bar(a INT, d INT);
				CREATE INDEX idx_foo_a_d ON foo(a, d);
				CREATE INDEX idx_bar_a_d ON bar(a, d);
			`)

		got, err := planner.Optimize(
			stream.New(stream.Concat(
				stream.New(table.Scan("foo")).
					Pipe(rows.Filter(parser.MustParseExpr("a = 1"))).
					Pipe(rows.Filter(parser.MustParseExpr("d = 2"))),
				stream.New(table.Scan("bar")).
					Pipe(rows.Filter(parser.MustParseExpr("a = 1"))).
					Pipe(rows.Filter(parser.MustParseExpr("d = 2"))),
			)),
			tx.Catalog, nil)

		want := stream.New(stream.Concat(
			stream.New(index.Scan("idx_foo_a_d", stream.Range{Min: testutil.ExprList(t, `(1, 2)`), Exact: true})),
			stream.New(index.Scan("idx_bar_a_d", stream.Range{Min: testutil.ExprList(t, `(1, 2)`), Exact: true})),
		))

		require.NoError(t, err)
		require.Equal(t, want.String(), got.String())
	})
}
