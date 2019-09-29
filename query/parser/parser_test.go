package parser

import (
	"strings"
	"testing"

	"github.com/asdine/genji/query"
	"github.com/asdine/genji/query/expr"
	"github.com/asdine/genji/query/q"
	"github.com/stretchr/testify/require"
)

func TestParserExpr(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected expr.Expr
	}{
		{"=", "age = 10", expr.Eq(q.Field("age"), expr.Int64Value(10))},
		{"AND", "age = 10 AND age <= 11",
			expr.And(
				expr.Eq(q.Field("age"), expr.Int64Value(10)),
				expr.Lte(q.Field("age"), expr.Int64Value(11)),
			)},
		{"OR", "age = 10 OR age = 11",
			expr.Or(
				expr.Eq(q.Field("age"), expr.Int64Value(10)),
				expr.Eq(q.Field("age"), expr.Int64Value(11)),
			)},
		{"AND then OR", "age >= 10 AND age > $age OR age < 10.4",
			expr.Or(
				expr.And(
					expr.Gte(q.Field("age"), expr.Int64Value(10)),
					expr.Gt(q.Field("age"), expr.NamedParam("age")),
				),
				expr.Lt(q.Field("age"), expr.Float64Value(10.4)),
			)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ex, err := NewParser(strings.NewReader(test.s)).ParseExpr()
			require.NoError(t, err)
			require.EqualValues(t, test.expected, ex)
		})
	}
}

func TestParserParams(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected expr.Expr
		errored  bool
	}{
		{"one positional", "age = ?", expr.Eq(q.Field("age"), expr.PositionalParam(1)), false},
		{"multiple positional", "age = ? AND age <= ?",
			expr.And(
				expr.Eq(q.Field("age"), expr.PositionalParam(1)),
				expr.Lte(q.Field("age"), expr.PositionalParam(2)),
			), false},
		{"one named", "age = $age", expr.Eq(q.Field("age"), expr.NamedParam("age")), false},
		{"multiple named", "age = $foo OR age = $bar",
			expr.Or(
				expr.Eq(q.Field("age"), expr.NamedParam("foo")),
				expr.Eq(q.Field("age"), expr.NamedParam("bar")),
			), false},
		{"mixed", "age >= ? AND age > $foo OR age < ?", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ex, err := NewParser(strings.NewReader(test.s)).ParseExpr()
			if test.errored {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, test.expected, ex)
			}
		})
	}
}

func TestParserMultiStatement(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected []query.Statement
	}{
		{"OnlyCommas", ";;;", nil},
		{"TrailingComma", "SELECT * FROM foo;;;DELETE FROM foo;", []query.Statement{
			query.Select().From(q.Table("foo")),
			query.Delete().From(q.Table("foo")),
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
