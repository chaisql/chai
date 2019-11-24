package parser

import (
	"testing"

	"github.com/asdine/genji/query"
	"github.com/stretchr/testify/require"
)

func TestParserInsert(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Statement
		fails    bool
	}{
		{"Values / No columns", `INSERT INTO test VALUES ('a', -1, true)`,
			query.InsertStmt{TableName: "test", Values: query.LiteralExprList{query.LiteralExprList{query.StringValue("a"), query.Int8Value(-1), query.BoolValue(true)}}}, false},
		{"Values / With columns", "INSERT INTO test (a, b) VALUES ('c', 'd', 'e')",
			query.InsertStmt{
				TableName:  "test",
				FieldNames: []string{"a", "b"},
				Values: query.LiteralExprList{
					query.LiteralExprList{query.StringValue("c"), query.StringValue("d"), query.StringValue("e")},
				},
			}, false},
		{"Values / Multiple", "INSERT INTO test (a, b) VALUES ('c', 'd'), ('e', 'f')",
			query.InsertStmt{
				TableName:  "test",
				FieldNames: []string{"a", "b"},
				Values: query.LiteralExprList{
					query.LiteralExprList{query.StringValue("c"), query.StringValue("d")},
					query.LiteralExprList{query.StringValue("e"), query.StringValue("f")},
				},
			}, false},

		{"Records", `INSERT INTO test RECORDS (a: 'a', b: 2.3, "c ": 1 = 1)`,
			query.InsertStmt{
				TableName: "test",
				Records: []interface{}{
					[]query.KVPair{
						query.KVPair{K: "a", V: query.StringValue("a")},
						query.KVPair{K: "b", V: query.Float64Value(2.3)},
						query.KVPair{K: "c ", V: query.Eq(query.Int8Value(1), query.Int8Value(1))},
					},
				},
			}, false},
		{"Records / Multiple", `INSERT INTO test RECORDS ("a": 'a', b: -2.3), (a: 1, d: true)`,
			query.InsertStmt{
				TableName: "test",
				Records: []interface{}{
					[]query.KVPair{
						query.KVPair{K: "a", V: query.StringValue("a")},
						query.KVPair{K: "b", V: query.Float64Value(-2.3)},
					},
					[]query.KVPair{query.KVPair{K: "a", V: query.Int8Value(1)}, query.KVPair{K: "d", V: query.BoolValue(true)}},
				},
			}, false},
		{"Records / Positional Param", "INSERT INTO test RECORDS ?, ?",
			query.InsertStmt{
				TableName: "test",
				Records:   []interface{}{query.PositionalParam(1), query.PositionalParam(2)},
			},
			false},
		{"Records / Named Param", "INSERT INTO test RECORDS $foo, $bar",
			query.InsertStmt{
				TableName: "test",
				Records:   []interface{}{query.NamedParam("foo"), query.NamedParam("bar")},
			},
			false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parseQuery(test.s)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}
