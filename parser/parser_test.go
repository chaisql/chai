package parser

import (
	"strings"
	"testing"

	"github.com/asdine/genji/query"
	"github.com/stretchr/testify/require"
)

func TestParserExpr(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Expr
	}{
		{"=", "age = 10", query.Eq(query.FieldSelector("age"), query.Int8Value(10))},
		{"!=", "age != 10", query.Neq(query.FieldSelector("age"), query.Int8Value(10))},
		{">", "age > 10", query.Gt(query.FieldSelector("age"), query.Int8Value(10))},
		{">=", "age >= 10", query.Gte(query.FieldSelector("age"), query.Int8Value(10))},
		{"<", "age < 10", query.Lt(query.FieldSelector("age"), query.Int8Value(10))},
		{"<=", "age <= 10", query.Lte(query.FieldSelector("age"), query.Int8Value(10))},
		{"AND", "age = 10 AND age <= 11",
			query.And(
				query.Eq(query.FieldSelector("age"), query.Int8Value(10)),
				query.Lte(query.FieldSelector("age"), query.Int8Value(11)),
			)},
		{"OR", "age = 10 OR age = 11",
			query.Or(
				query.Eq(query.FieldSelector("age"), query.Int8Value(10)),
				query.Eq(query.FieldSelector("age"), query.Int8Value(11)),
			)},
		{"AND then OR", "age >= 10 AND age > $age OR age < 10.4",
			query.Or(
				query.And(
					query.Gte(query.FieldSelector("age"), query.Int8Value(10)),
					query.Gt(query.FieldSelector("age"), query.NamedParam("age")),
				),
				query.Lt(query.FieldSelector("age"), query.Float64Value(10.4)),
			)},
		{"with NULL", "age > NULL", query.Gt(query.FieldSelector("age"), query.NullValue())},
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
		expected query.Expr
		errored  bool
	}{
		{"one positional", "age = ?", query.Eq(query.FieldSelector("age"), query.PositionalParam(1)), false},
		{"multiple positional", "age = ? AND age <= ?",
			query.And(
				query.Eq(query.FieldSelector("age"), query.PositionalParam(1)),
				query.Lte(query.FieldSelector("age"), query.PositionalParam(2)),
			), false},
		{"one named", "age = $age", query.Eq(query.FieldSelector("age"), query.NamedParam("age")), false},
		{"multiple named", "age = $foo OR age = $bar",
			query.Or(
				query.Eq(query.FieldSelector("age"), query.NamedParam("foo")),
				query.Eq(query.FieldSelector("age"), query.NamedParam("bar")),
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
			query.SelectStmt{Selectors: []query.ResultField{query.Wildcard{}}, TableName: "foo"},
			query.DeleteStmt{TableName: "foo"},
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
