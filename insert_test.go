package genji

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParserInsert(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected Statement
		errored  bool
	}{
		{"Values / No columns", "INSERT INTO test VALUES ('a', 'b', 'c')",
			insertStmt{tableName: "test", values: LitteralExprList{LitteralExprList{StringValue("a"), StringValue("b"), StringValue("c")}}}, false},
		{"Values / With columns", "INSERT INTO test (a, b) VALUES ('c', 'd', 'e')",
			insertStmt{
				tableName:  "test",
				fieldNames: []string{"a", "b"},
				values: LitteralExprList{
					LitteralExprList{StringValue("c"), StringValue("d"), StringValue("e")},
				},
			}, false},
		{"Values / Multple", "INSERT INTO test (a, b) VALUES ('c', 'd'), ('e', 'f')",
			insertStmt{
				tableName:  "test",
				fieldNames: []string{"a", "b"},
				values: LitteralExprList{
					LitteralExprList{StringValue("c"), StringValue("d")},
					LitteralExprList{StringValue("e"), StringValue("f")},
				},
			}, false},

		{"Records", "INSERT INTO test RECORDS (a: 'a', b: 2.3, c: 1 = 1)",
			insertStmt{
				tableName: "test",
				records: []interface{}{
					[]kvPair{
						kvPair{K: "a", V: StringValue("a")},
						kvPair{K: "b", V: Float64Value(2.3)},
						kvPair{K: "c", V: Eq(Int64Value(1), Int64Value(1))},
					},
				},
			}, false},
		{"Records / Multiple", "INSERT INTO test RECORDS (a: 'a', b: 2.3), (a: 1, d: true)",
			insertStmt{
				tableName: "test",
				records: []interface{}{
					[]kvPair{
						kvPair{K: "a", V: StringValue("a")},
						kvPair{K: "b", V: Float64Value(2.3)},
					},
					[]kvPair{kvPair{K: "a", V: Int64Value(1)}, kvPair{K: "d", V: BoolValue(true)}},
				},
			}, false},
		{"Records / Positional Param", "INSERT INTO test RECORDS ?, ?",
			insertStmt{
				tableName: "test",
				records:   []interface{}{PositionalParam(1), PositionalParam(2)},
			},
			false},
		{"Records / Named Param", "INSERT INTO test RECORDS $foo, $bar",
			insertStmt{
				tableName: "test",
				records:   []interface{}{NamedParam("foo"), NamedParam("bar")},
			},
			false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := ParseQuery(test.s)
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
