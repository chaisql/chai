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
		expected expr
	}{
		{"=", "age = 10", eq(fieldSelector("age"), int8Value(10))},
		{"!=", "age != 10", neq(fieldSelector("age"), int8Value(10))},
		{">", "age > 10", gt(fieldSelector("age"), int8Value(10))},
		{">=", "age >= 10", gte(fieldSelector("age"), int8Value(10))},
		{"<", "age < 10", lt(fieldSelector("age"), int8Value(10))},
		{"<=", "age <= 10", lte(fieldSelector("age"), int8Value(10))},
		{"AND", "age = 10 AND age <= 11",
			and(
				eq(fieldSelector("age"), int8Value(10)),
				lte(fieldSelector("age"), int8Value(11)),
			)},
		{"OR", "age = 10 OR age = 11",
			or(
				eq(fieldSelector("age"), int8Value(10)),
				eq(fieldSelector("age"), int8Value(11)),
			)},
		{"AND then OR", "age >= 10 AND age > $age OR age < 10.4",
			or(
				and(
					gte(fieldSelector("age"), int8Value(10)),
					gt(fieldSelector("age"), namedParam("age")),
				),
				lt(fieldSelector("age"), float64Value(10.4)),
			)},
		{"with NULL", "age > NULL", gt(fieldSelector("age"), nullValue())},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ex, err := newParser(strings.NewReader(test.s)).ParseExpr()
			require.NoError(t, err)
			require.EqualValues(t, test.expected, ex)
		})
	}
}

func TestParserParams(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected expr
		errored  bool
	}{
		{"one positional", "age = ?", eq(fieldSelector("age"), positionalParam(1)), false},
		{"multiple positional", "age = ? AND age <= ?",
			and(
				eq(fieldSelector("age"), positionalParam(1)),
				lte(fieldSelector("age"), positionalParam(2)),
			), false},
		{"one named", "age = $age", eq(fieldSelector("age"), namedParam("age")), false},
		{"multiple named", "age = $foo OR age = $bar",
			or(
				eq(fieldSelector("age"), namedParam("foo")),
				eq(fieldSelector("age"), namedParam("bar")),
			), false},
		{"mixed", "age >= ? AND age > $foo OR age < ?", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ex, err := newParser(strings.NewReader(test.s)).ParseExpr()
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
		expected []statement
	}{
		{"OnlyCommas", ";;;", nil},
		{"TrailingComma", "SELECT * FROM foo;;;DELETE FROM foo;", []statement{
			selectStmt{selectors: []resultField{wildcard{}}, tableName: "foo"},
			deleteStmt{tableName: "foo"},
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parseQuery(test.s)
			require.NoError(t, err)
			require.EqualValues(t, test.expected, q.Statements)
		})
	}
}
