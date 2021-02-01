package parser

import (
	"testing"

	"github.com/genjidb/genji/sql/planner"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/stream"
	"github.com/stretchr/testify/require"
)

func TestParserInsert(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected *stream.Stream
		fails    bool
	}{
		{"Documents", `INSERT INTO test VALUES {a: 1, "b": "foo", c: 'bar', d: 1 = 1, e: {f: "baz"}}`,
			stream.New(stream.Expressions(
				&expr.KVPairs{SelfReferenced: true, Pairs: []expr.KVPair{
					{K: "a", V: expr.IntegerValue(1)},
					{K: "b", V: expr.TextValue("foo")},
					{K: "c", V: expr.TextValue("bar")},
					{K: "d", V: expr.Eq(expr.IntegerValue(1), expr.IntegerValue(1))},
					{K: "e", V: &expr.KVPairs{SelfReferenced: true, Pairs: []expr.KVPair{
						{K: "f", V: expr.TextValue("baz")},
					}}},
				}},
			)).Pipe(stream.TableInsert("test")),
			false},
		{"Documents / Multiple", `INSERT INTO test VALUES {"a": 'a', b: -2.3}, {a: 1, d: true}`,
			stream.New(stream.Expressions(
				&expr.KVPairs{SelfReferenced: true, Pairs: []expr.KVPair{
					{K: "a", V: expr.TextValue("a")},
					{K: "b", V: expr.DoubleValue(-2.3)},
				}},
				&expr.KVPairs{SelfReferenced: true, Pairs: []expr.KVPair{{K: "a", V: expr.IntegerValue(1)}, {K: "d", V: expr.BoolValue(true)}}},
			)).Pipe(stream.TableInsert("test")),
			false},
		{"Documents / Positional Param", "INSERT INTO test VALUES ?, ?",
			stream.New(stream.Expressions(
				expr.PositionalParam(1),
				expr.PositionalParam(2),
			)).Pipe(stream.TableInsert("test")),
			false},
		{"Documents / Named Param", "INSERT INTO test VALUES $foo, $bar",
			stream.New(stream.Expressions(
				expr.NamedParam("foo"),
				expr.NamedParam("bar"),
			)).Pipe(stream.TableInsert("test")),
			false},
		{"Values / With fields", "INSERT INTO test (a, b) VALUES ('c', 'd')",
			stream.New(stream.Expressions(
				&expr.KVPairs{Pairs: []expr.KVPair{
					{K: "a", V: expr.TextValue("c")},
					{K: "b", V: expr.TextValue("d")},
				}},
			)).Pipe(stream.TableInsert("test")),
			false},
		{"Values / With too many values", "INSERT INTO test (a, b) VALUES ('c', 'd', 'e')",
			nil, true},
		{"Values / Multiple", "INSERT INTO test (a, b) VALUES ('c', 'd'), ('e', 'f')",
			stream.New(stream.Expressions(
				&expr.KVPairs{Pairs: []expr.KVPair{
					{K: "a", V: expr.TextValue("c")},
					{K: "b", V: expr.TextValue("d")},
				}},
				&expr.KVPairs{Pairs: []expr.KVPair{
					{K: "a", V: expr.TextValue("e")},
					{K: "b", V: expr.TextValue("f")},
				}},
			)).Pipe(stream.TableInsert("test")),
			false},
		{"Values / With fields / Wrong values", "INSERT INTO test (a, b) VALUES {a: 1}, ('e', 'f')",
			nil, true},
		{"Values / Without fields / Wrong values", "INSERT INTO test VALUES {a: 1}, ('e', 'f')",
			nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := ParseQuery(test.s)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, q.Statements, 1)
			stmt := q.Statements[0].(*planner.Statement)
			require.False(t, stmt.ReadOnly)
			require.EqualValues(t, test.expected, stmt.Stream)
		})
	}
}
