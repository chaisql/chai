package expr_test

import (
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/stretchr/testify/require"
)

func TestComparisonExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   document.Value
		fails bool
	}{
		{"1 = a", document.NewBoolValue(true), false},
		{"1 = NULL", nullLitteral, false},
		{"1 = notFound", nullLitteral, false},
		{"1 != a", document.NewBoolValue(false), false},
		{"1 != NULL", nullLitteral, false},
		{"1 != notFound", nullLitteral, false},
		{"1 > a", document.NewBoolValue(false), false},
		{"1 > NULL", nullLitteral, false},
		{"1 > notFound", nullLitteral, false},
		{"1 >= a", document.NewBoolValue(true), false},
		{"1 >= NULL", nullLitteral, false},
		{"1 >= notFound", nullLitteral, false},
		{"1 < a", document.NewBoolValue(false), false},
		{"1 < NULL", nullLitteral, false},
		{"1 < notFound", nullLitteral, false},
		{"1 <= a", document.NewBoolValue(true), false},
		{"1 <= NULL", nullLitteral, false},
		{"1 <= notFound", nullLitteral, false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testExpr(t, test.expr, envWithDoc, test.res, test.fails)
		})
	}
}

func TestComparisonINExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   document.Value
		fails bool
	}{
		{"1 IN []", document.NewBoolValue(false), false},
		{"1 IN [1, 2, 3]", document.NewBoolValue(true), false},
		{"2 IN [2.1, 2.2, 2.0]", document.NewBoolValue(true), false},
		{"1 IN [2, 3]", document.NewBoolValue(false), false},
		{"[1] IN [1, 2, 3]", document.NewBoolValue(false), false},
		{"[1] IN [[1], [2], [3]]", document.NewBoolValue(true), false},
		{"1 IN {}", document.NewBoolValue(false), false},
		{"[1, 2] IN 1", document.NewBoolValue(false), false},
		{"1 IN NULL", nullLitteral, false},
		{"NULL IN [1, 2, NULL]", nullLitteral, false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testExpr(t, test.expr, envWithDoc, test.res, test.fails)
		})
	}
}

func TestComparisonNOTINExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   document.Value
		fails bool
	}{
		{"1 NOT IN []", document.NewBoolValue(true), false},
		{"1 NOT IN [1, 2, 3]", document.NewBoolValue(false), false},
		{"1 NOT IN [2, 3]", document.NewBoolValue(true), false},
		{"[1] NOT IN [1, 2, 3]", document.NewBoolValue(true), false},
		{"[1] NOT IN [[1], [2], [3]]", document.NewBoolValue(false), false},
		{"1 NOT IN {}", document.NewBoolValue(true), false},
		{"[1, 2] NOT IN 1", document.NewBoolValue(true), false},
		{"1 NOT IN NULL", nullLitteral, false},
		{"NULL NOT IN [1, 2, NULL]", nullLitteral, false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testExpr(t, test.expr, envWithDoc, test.res, test.fails)
		})
	}
}

func TestComparisonISExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   document.Value
		fails bool
	}{
		{"1 IS 1", document.NewBoolValue(true), false},
		{"1 IS 2", document.NewBoolValue(false), false},
		{"1 IS NULL", document.NewBoolValue(false), false},
		{"NULL IS NULL", document.NewBoolValue(true), false},
		{"NULL IS 1", document.NewBoolValue(false), false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testExpr(t, test.expr, envWithDoc, test.res, test.fails)
		})
	}
}

func TestComparisonISNOTExpr(t *testing.T) {
	tests := []struct {
		expr  string
		res   document.Value
		fails bool
	}{
		{"1 IS NOT 1", document.NewBoolValue(false), false},
		{"1 IS NOT 2", document.NewBoolValue(true), false},
		{"1 IS NOT NULL", document.NewBoolValue(true), false},
		{"NULL IS NOT NULL", document.NewBoolValue(false), false},
		{"NULL IS NOT 1", document.NewBoolValue(true), false},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			testExpr(t, test.expr, envWithDoc, test.res, test.fails)
		})
	}
}

func TestComparisonExprNodocument(t *testing.T) {
	tests := []struct {
		expr  string
		res   document.Value
		fails bool
	}{
		{"1 = a", nullLitteral, true},
		{"1 != a", nullLitteral, true},
		{"1 > a", nullLitteral, true},
		{"1 >= a", nullLitteral, true},
		{"1 < a", nullLitteral, true},
		{"1 <= a", nullLitteral, true},
		{"1 IN [a]", nullLitteral, true},
		{"1 IS a", nullLitteral, true},
		{"1 IS NOT a", nullLitteral, true},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.expr, func(t *testing.T) {
					var emptyenv expr.Environment

					testExpr(t, test.expr, &emptyenv, test.res, test.fails)
				})
			}
		})
	}
}

func TestIndexedComparisonExpr(t *testing.T) {
	type idxOp interface {
		IterateIndex(idx *database.Index, tb *database.Table, v document.Value, fn func(d document.Document) error) error
	}

	tests := []struct {
		op       expr.Expr
		v        document.Value
		expected []interface{}
		fails    bool
	}{
		{expr.Eq(nil, nil), document.NewDoubleValue(5), []interface{}{5.0}, false},
		{expr.Eq(nil, nil), document.NewDoubleValue(50), nil, false},
		{expr.Gt(nil, nil), document.NewDoubleValue(0), []interface{}{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0}, false},
		{expr.Gt(nil, nil), document.NewDoubleValue(10), nil, false},
		{expr.Gt(nil, nil), document.NewDoubleValue(-100), []interface{}{-10.0, -9.0, -8.0, -7.0, -6.0, -5.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0}, false},
		{expr.Gte(nil, nil), document.NewDoubleValue(0), []interface{}{0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0}, false},
		{expr.Gte(nil, nil), document.NewDoubleValue(10), nil, false},
		{expr.Gte(nil, nil), document.NewDoubleValue(-100), []interface{}{-10.0, -9.0, -8.0, -7.0, -6.0, -5.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0}, false},
		{expr.Lt(nil, nil), document.NewDoubleValue(0), []interface{}{-10.0, -9.0, -8.0, -7.0, -6.0, -5.0, -4.0, -3.0, -2.0, -1.0}, false},
		{expr.Lt(nil, nil), document.NewDoubleValue(-11), nil, false},
		{expr.Lt(nil, nil), document.NewDoubleValue(100), []interface{}{-10.0, -9.0, -8.0, -7.0, -6.0, -5.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0}, false},
		{expr.Lte(nil, nil), document.NewDoubleValue(0), []interface{}{-10.0, -9.0, -8.0, -7.0, -6.0, -5.0, -4.0, -3.0, -2.0, -1.0, 0.0}, false},
		{expr.Lte(nil, nil), document.NewDoubleValue(-11), nil, false},
		{expr.Lte(nil, nil), document.NewDoubleValue(100), []interface{}{-10.0, -9.0, -8.0, -7.0, -6.0, -5.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0}, false},
	}

	db, err := genji.Open(":memory:")
	require.NoError(t, err)
	defer db.Close()

	err = db.Update(func(tx *genji.Tx) error {
		err = tx.Exec("CREATE TABLE foo; CREATE INDEX idx_foo ON foo(a)")
		if err != nil {
			return err
		}
		for i := -10; i < 10; i++ {
			err = tx.Exec("INSERT INTO foo(a) VALUES (?)", i)
			if err != nil {
				return err
			}
		}

		return nil
	})
	require.NoError(t, err)

	for _, test := range tests {
		t.Run(test.op.(expr.Operator).Token().String(), func(t *testing.T) {
			tx, err := db.Begin(true)
			require.NoError(t, err)
			defer tx.Rollback()

			tb, err := tx.GetTable("foo")
			require.NoError(t, err)
			idx, err := tx.GetIndex("idx_foo")
			require.NoError(t, err)

			var docs []interface{}

			err = test.op.(idxOp).IterateIndex(idx, tb, test.v, func(d document.Document) error {
				v, err := d.GetByField("a")
				if err != nil {
					return err
				}
				docs = append(docs, v.V)
				return nil
			})
			require.NoError(t, err)

			require.Equal(t, test.expected, docs)
		})
	}
}
