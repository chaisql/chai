package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParser(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected Statement
	}{
		{"=", "SELECT FROM test WHERE age = 10", Select().From(Table("test")).Where(Eq(Field("age"), Int64Value(10)))},
		{"AND", "SELECT FROM test WHERE age = 10 AND age <= 11",
			Select().From(Table("test")).Where(And(
				Eq(Field("age"), Int64Value(10)),
				Lte(Field("age"), Int64Value(11)),
			))},
		{"OR", "SELECT FROM test WHERE age = 10 OR age = 11",
			Select().From(Table("test")).Where(Or(
				Eq(Field("age"), Int64Value(10)),
				Eq(Field("age"), Int64Value(11)),
			))},
		{"AND then OR", "SELECT FROM test WHERE age >= 10 AND age > 11 OR age < 10.4",
			Select().From(Table("test")).Where(Or(
				And(
					Gte(Field("age"), Int64Value(10)),
					Gt(Field("age"), Int64Value(11)),
				),
				Lt(Field("age"), Float64Value(10.4)),
			))},
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
