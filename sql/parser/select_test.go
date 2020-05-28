package parser

import (
	"testing"

	"github.com/genjidb/genji/sql/query"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/sql/scanner"
	"github.com/stretchr/testify/require"
)

func TestParserSelect(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Statement
		mustFail bool
	}{
		{"NoTable", "SELECT 1",
			query.SelectStmt{
				Selectors: []query.ResultField{query.ResultFieldExpr{Expr: expr.IntValue(1), ExprName: "1"}},
			}, false},
		{"NoCond", "SELECT * FROM test",
			query.SelectStmt{
				Selectors: []query.ResultField{query.Wildcard{}},
				TableName: "test",
			}, false},
		{"WithFields", "SELECT a, b FROM test",
			query.SelectStmt{
				Selectors: []query.ResultField{query.ResultFieldExpr{Expr: expr.FieldSelector([]string{"a"}), ExprName: "a"}, query.ResultFieldExpr{Expr: expr.FieldSelector([]string{"b"}), ExprName: "b"}},
				TableName: "test",
			}, false},
		{"WithAlias", "SELECT a AS A, b FROM test",
			query.SelectStmt{
				Selectors: []query.ResultField{query.ResultFieldExpr{Expr: expr.FieldSelector([]string{"a"}), ExprName: "A"}, query.ResultFieldExpr{Expr: expr.FieldSelector([]string{"b"}), ExprName: "b"}},
				TableName: "test",
			}, false},
		{"WithFields and wildcard", "SELECT a, b, * FROM test",
			query.SelectStmt{
				Selectors: []query.ResultField{query.ResultFieldExpr{Expr: expr.FieldSelector([]string{"a"}), ExprName: "a"}, query.ResultFieldExpr{Expr: expr.FieldSelector([]string{"b"}), ExprName: "b"}, query.Wildcard{}},
				TableName: "test",
			}, false},
		{"WithExpr", "SELECT a    > 1 FROM test",
			query.SelectStmt{
				Selectors: []query.ResultField{query.ResultFieldExpr{Expr: expr.Gt(expr.FieldSelector([]string{"a"}), expr.IntValue(1)), ExprName: "a    > 1"}},
				TableName: "test",
			}, false},
		{"WithCond", "SELECT * FROM test WHERE age = 10",
			query.SelectStmt{
				TableName: "test",
				Selectors: []query.ResultField{query.Wildcard{}},
				WhereExpr: expr.Eq(expr.FieldSelector([]string{"age"}), expr.IntValue(10)),
			}, false},
		{"WithOrderBy", "SELECT * FROM test WHERE age = 10 ORDER BY a.b.c",
			query.SelectStmt{
				TableName: "test",
				Selectors: []query.ResultField{query.Wildcard{}},
				WhereExpr: expr.Eq(expr.FieldSelector([]string{"age"}), expr.IntValue(10)),
				OrderBy:   []string{"a", "b", "c"},
			}, false},
		{"WithOrderBy ASC", "SELECT * FROM test WHERE age = 10 ORDER BY a.b.c ASC",
			query.SelectStmt{
				TableName:        "test",
				Selectors:        []query.ResultField{query.Wildcard{}},
				WhereExpr:        expr.Eq(expr.FieldSelector([]string{"age"}), expr.IntValue(10)),
				OrderBy:          []string{"a", "b", "c"},
				OrderByDirection: scanner.ASC,
			}, false},
		{"WithOrderBy DESC", "SELECT * FROM test WHERE age = 10 ORDER BY a.b.c DESC",
			query.SelectStmt{
				TableName:        "test",
				Selectors:        []query.ResultField{query.Wildcard{}},
				WhereExpr:        expr.Eq(expr.FieldSelector([]string{"age"}), expr.IntValue(10)),
				OrderBy:          []string{"a", "b", "c"},
				OrderByDirection: scanner.DESC,
			}, false},
		{"WithLimit", "SELECT * FROM test WHERE age = 10 LIMIT 20",
			query.SelectStmt{
				Selectors: []query.ResultField{query.Wildcard{}},
				TableName: "test",
				WhereExpr: expr.Eq(expr.FieldSelector([]string{"age"}), expr.IntValue(10)),
				LimitExpr: expr.IntValue(20),
			}, false},
		{"WithOffset", "SELECT * FROM test WHERE age = 10 OFFSET 20",
			query.SelectStmt{
				Selectors:  []query.ResultField{query.Wildcard{}},
				TableName:  "test",
				WhereExpr:  expr.Eq(expr.FieldSelector([]string{"age"}), expr.IntValue(10)),
				OffsetExpr: expr.IntValue(20),
			}, false},
		{"WithLimitThenOffset", "SELECT * FROM test WHERE age = 10 LIMIT 10 OFFSET 20",
			query.SelectStmt{
				Selectors:  []query.ResultField{query.Wildcard{}},
				TableName:  "test",
				WhereExpr:  expr.Eq(expr.FieldSelector([]string{"age"}), expr.IntValue(10)),
				OffsetExpr: expr.IntValue(20),
				LimitExpr:  expr.IntValue(10),
			}, false},
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
