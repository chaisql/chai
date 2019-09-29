package parser

import (
	"testing"

	"github.com/asdine/genji/query"
	"github.com/asdine/genji/query/expr"
	"github.com/asdine/genji/query/q"
	"github.com/stretchr/testify/require"
)

func TestParserInsert(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Statement
		errored  bool
	}{
		{"Values / No columns", "INSERT INTO test VALUES ('a', 'b', 'c')", query.Insert().Into(q.Table("test")).Values(expr.StringValue("a"), expr.StringValue("b"), expr.StringValue("c")), false},
		{"Values / With columns", "INSERT INTO test (a, b) VALUES ('c', 'd', 'e')",
			query.Insert().Into(q.Table("test")).
				Fields("a", "b").
				Values(expr.StringValue("c"), expr.StringValue("d"), expr.StringValue("e")), false},
		{"Values / Multple", "INSERT INTO test (a, b) VALUES ('c', 'd'), ('e', 'f')",
			query.Insert().Into(q.Table("test")).
				Fields("a", "b").
				Values(expr.StringValue("c"), expr.StringValue("d")).
				Values(expr.StringValue("e"), expr.StringValue("f")), false},
		{"Records", "INSERT INTO test RECORDS (a: 'a', b: 2.3, c: 1 = 1)",
			query.Insert().Into(q.Table("test")).
				Pairs(query.KVPair{K: "a", V: expr.StringValue("a")}, query.KVPair{K: "b", V: expr.Float64Value(2.3)}, query.KVPair{K: "c", V: expr.Eq(expr.Int64Value(1), expr.Int64Value(1))}), false},
		{"Records / Multiple", "INSERT INTO test RECORDS (a: 'a', b: 2.3, c: 1 = 1), (a: 1, d: true)",
			query.Insert().Into(q.Table("test")).
				Pairs(query.KVPair{K: "a", V: expr.StringValue("a")}, query.KVPair{K: "b", V: expr.Float64Value(2.3)}, query.KVPair{K: "c", V: expr.Eq(expr.Int64Value(1), expr.Int64Value(1))}).
				Pairs(query.KVPair{K: "a", V: expr.Int64Value(1)}, query.KVPair{K: "d", V: expr.BoolValue(true)}), false},
		{"Records / Positional Param", "INSERT INTO test RECORDS ?, ?",
			query.Insert().Into(q.Table("test")).
				Records(expr.PositionalParam(1), expr.PositionalParam(2)),
			false},
		{"Records / Named Param", "INSERT INTO test RECORDS $foo, $bar",
			query.Insert().Into(q.Table("test")).
				Records(expr.NamedParam("foo"), expr.NamedParam("bar")),
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
