package genji

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParserExpr(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected Expr
	}{
		{"=", "age = 10", Eq(FieldSelector("age"), Int64Value(10))},
		{"AND", "age = 10 AND age <= 11",
			And(
				Eq(FieldSelector("age"), Int64Value(10)),
				Lte(FieldSelector("age"), Int64Value(11)),
			)},
		{"OR", "age = 10 OR age = 11",
			Or(
				Eq(FieldSelector("age"), Int64Value(10)),
				Eq(FieldSelector("age"), Int64Value(11)),
			)},
		{"AND then OR", "age >= 10 AND age > $age OR age < 10.4",
			Or(
				And(
					Gte(FieldSelector("age"), Int64Value(10)),
					Gt(FieldSelector("age"), NamedParam("age")),
				),
				Lt(FieldSelector("age"), Float64Value(10.4)),
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
		expected Expr
		errored  bool
	}{
		{"one positional", "age = ?", Eq(FieldSelector("age"), PositionalParam(1)), false},
		{"multiple positional", "age = ? AND age <= ?",
			And(
				Eq(FieldSelector("age"), PositionalParam(1)),
				Lte(FieldSelector("age"), PositionalParam(2)),
			), false},
		{"one named", "age = $age", Eq(FieldSelector("age"), NamedParam("age")), false},
		{"multiple named", "age = $foo OR age = $bar",
			Or(
				Eq(FieldSelector("age"), NamedParam("foo")),
				Eq(FieldSelector("age"), NamedParam("bar")),
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
		expected []Statement
	}{
		{"OnlyCommas", ";;;", nil},
		{"TrailingComma", "SELECT * FROM foo;;;DELETE FROM foo;", []Statement{
			selectStmt{tableSelector: tableSelector("foo")},
			deleteStmt{tableName: "foo"},
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
