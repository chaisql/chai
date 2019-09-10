package query

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
		{"=", "age = 10", Eq(Field("age"), Int64Value(10))},
		{"AND", "age = 10 AND age <= 11",
			And(
				Eq(Field("age"), Int64Value(10)),
				Lte(Field("age"), Int64Value(11)),
			)},
		{"OR", "age = 10 OR age = 11",
			Or(
				Eq(Field("age"), Int64Value(10)),
				Eq(Field("age"), Int64Value(11)),
			)},
		{"AND then OR", "age >= 10 AND age > 11 OR age < 10.4",
			Or(
				And(
					Gte(Field("age"), Int64Value(10)),
					Gt(Field("age"), Int64Value(11)),
				),
				Lt(Field("age"), Float64Value(10.4)),
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

func TestParserMultiStatement(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected []Statement
	}{
		{"OnlyCommas", ";;;", nil},
		{"TrailingComma", "SELECT FROM foo;;;DELETE FROM foo;", []Statement{
			Select().From(Table("foo")),
			Delete().From(Table("foo")),
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := ParseQuery(test.s)
			require.NoError(t, err)
			require.EqualValues(t, test.expected, q.statements)
		})
	}
}

func TestParserSelect(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected Statement
	}{
		{"NoCond", "SELECT FROM test", Select().From(Table("test"))},
		{"WithCond", "SELECT FROM test WHERE age = 10", Select().From(Table("test")).Where(Eq(Field("age"), Int64Value(10)))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := ParseQuery(test.s)
			require.NoError(t, err)
			require.Len(t, q.statements, 1)
			require.EqualValues(t, test.expected, q.statements[0])
		})
	}
}

func TestParserDelete(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected Statement
	}{
		{"NoCond", "DELETE FROM test", Delete().From(Table("test"))},
		{"WithCond", "DELETE FROM test WHERE age = 10", Delete().From(Table("test")).Where(Eq(Field("age"), Int64Value(10)))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := ParseQuery(test.s)
			require.NoError(t, err)
			require.Len(t, q.statements, 1)
			require.EqualValues(t, test.expected, q.statements[0])
		})
	}
}

func TestParserUdpate(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected Statement
		errored  bool
	}{
		{"No cond", "UPDATE test SET a = 1", Update(Table("test")).Set("a", Int64Value(1)), false},
		{"With cond", "UPDATE test SET a = 1, b = 2 WHERE age = 10", Update(Table("test")).Set("a", Int64Value(1)).Set("b", Int64Value(2)).Where(Eq(Field("age"), Int64Value(10))), false},
		{"Trailing comma", "UPDATE test SET a = 1, WHERE age = 10", nil, true},
		{"No SET", "UPDATE test WHERE age = 10", nil, true},
		{"No pair", "UPDATE test SET WHERE age = 10", nil, true},
		{"Field only", "UPDATE test SET a WHERE age = 10", nil, true},
		{"No value", "UPDATE test SET a = WHERE age = 10", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := ParseQuery(test.s)
			if test.errored {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, q.statements, 1)
			require.EqualValues(t, test.expected, q.statements[0])
		})
	}
}
