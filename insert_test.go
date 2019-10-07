package genji

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParserInsert(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected statement
		errored  bool
	}{
		{"Values / No columns", "INSERT INTO test VALUES ('a', 'b', 'c')",
			insertStmt{tableName: "test", values: litteralExprList{litteralExprList{stringValue("a"), stringValue("b"), stringValue("c")}}}, false},
		{"Values / With columns", "INSERT INTO test (a, b) VALUES ('c', 'd', 'e')",
			insertStmt{
				tableName:  "test",
				fieldNames: []string{"a", "b"},
				values: litteralExprList{
					litteralExprList{stringValue("c"), stringValue("d"), stringValue("e")},
				},
			}, false},
		{"Values / Multple", "INSERT INTO test (a, b) VALUES ('c', 'd'), ('e', 'f')",
			insertStmt{
				tableName:  "test",
				fieldNames: []string{"a", "b"},
				values: litteralExprList{
					litteralExprList{stringValue("c"), stringValue("d")},
					litteralExprList{stringValue("e"), stringValue("f")},
				},
			}, false},

		{"Records", "INSERT INTO test RECORDS (a: 'a', b: 2.3, c: 1 = 1)",
			insertStmt{
				tableName: "test",
				records: []interface{}{
					[]kvPair{
						kvPair{K: "a", V: stringValue("a")},
						kvPair{K: "b", V: float64Value(2.3)},
						kvPair{K: "c", V: eq(int64Value(1), int64Value(1))},
					},
				},
			}, false},
		{"Records / Multiple", "INSERT INTO test RECORDS (a: 'a', b: 2.3), (a: 1, d: true)",
			insertStmt{
				tableName: "test",
				records: []interface{}{
					[]kvPair{
						kvPair{K: "a", V: stringValue("a")},
						kvPair{K: "b", V: float64Value(2.3)},
					},
					[]kvPair{kvPair{K: "a", V: int64Value(1)}, kvPair{K: "d", V: boolValue(true)}},
				},
			}, false},
		{"Records / Positional Param", "INSERT INTO test RECORDS ?, ?",
			insertStmt{
				tableName: "test",
				records:   []interface{}{positionalParam(1), positionalParam(2)},
			},
			false},
		{"Records / Named Param", "INSERT INTO test RECORDS $foo, $bar",
			insertStmt{
				tableName: "test",
				records:   []interface{}{namedParam("foo"), namedParam("bar")},
			},
			false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parseQuery(test.s)
			if test.errored {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}
