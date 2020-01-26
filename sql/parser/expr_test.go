package parser

import (
	"strings"
	"testing"

	"github.com/asdine/genji/sql/query"
	"github.com/stretchr/testify/require"
)

func TestParserExpr(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Expr
		fails    bool
	}{
		// integers
		{"+int8", "10", query.Int8Value(10), false},
		{"-int8", "-10", query.Int8Value(-10), false},
		{"+int16", "1000", query.Int16Value(1000), false},
		{"-int16", "-1000", query.Int16Value(-1000), false},
		{"+int32", "10000000", query.Int32Value(10000000), false},
		{"-int32", "-10000000", query.Int32Value(-10000000), false},
		{"+int64", "10000000000", query.Int64Value(10000000000), false},
		{"-int64", "-10000000000", query.Int64Value(-10000000000), false},
		{"uint64", "10000000000000000000", query.Uint64Value(10000000000000000000), false},
		{"negative uint64", "-10000000000000000000", nil, true},
		{"int too large", "100000000000000000000000000000000000000000000000", nil, true},

		// floats
		{"+float64", "10.0", query.Float64Value(10), false},
		{"-float64", "-10.0", query.Float64Value(-10), false},

		// strings
		{"double quoted string", `"10.0"`, query.StringValue("10.0"), false},
		{"single quoted string", "'-10.0'", query.StringValue("-10.0"), false},

		// identifiers
		{"simple field ref", `a`, query.FieldSelector{"a"}, false},
		{"simple field ref with quotes", "`some ident`", query.FieldSelector{"some ident"}, false},
		{"field ref", `a.b.100.c.1.2.3`, query.FieldSelector{"a", "b", "100", "c", "1", "2", "3"}, false},
		{"field ref negative", `a.b.-100.c`, nil, true},
		{"field ref with spaces", `a.  b.100.  c`, nil, true},
		{"field ref with quotes", "`some ident`.` with`.5.`  quotes`", query.FieldSelector{"some ident", " with", "5", "  quotes"}, false},

		// documents
		{"empty document", `{}`, query.KVPairs(nil), false},
		{"document values", `{a: 1, b: 1.0, c: true, d: 'string', e: "string", f: {foo: 'bar'}, g: h.i.j, k: [1, 2, 3]}`,
			query.KVPairs{
				query.KVPair{K: "a", V: query.Int8Value(1)},
				query.KVPair{K: "b", V: query.Float64Value(1)},
				query.KVPair{K: "c", V: query.BoolValue(true)},
				query.KVPair{K: "d", V: query.StringValue("string")},
				query.KVPair{K: "e", V: query.StringValue("string")},
				query.KVPair{K: "f", V: query.KVPairs{
					query.KVPair{K: "foo", V: query.StringValue("bar")},
				}},
				query.KVPair{K: "g", V: query.FieldSelector([]string{"h", "i", "j"})},
				query.KVPair{K: "k", V: query.LiteralExprList{query.Int8Value(1), query.Int8Value(2), query.Int8Value(3)}},
			},
			false},
		{"document keys", `{a: 1, "foo bar __&&))": 1, 'ola ': 1}`,
			query.KVPairs{
				query.KVPair{K: "a", V: query.Int8Value(1)},
				query.KVPair{K: "foo bar __&&))", V: query.Int8Value(1)},
				query.KVPair{K: "ola ", V: query.Int8Value(1)},
			},
			false},
		{"document keys: same key", `{a: 1, a: 2, "a": 3}`,
			query.KVPairs{
				query.KVPair{K: "a", V: query.Int8Value(1)},
				query.KVPair{K: "a", V: query.Int8Value(2)},
				query.KVPair{K: "a", V: query.Int8Value(3)},
			},
			false},
		{"bad document keys: param", `{?: 1}`, nil, true},
		{"bad document keys: dot", `{a.b: 1}`, nil, true},
		{"bad document keys: space", `{a b: 1}`, nil, true},
		{"bad document: missing right bracket", `{a: 1`, nil, true},
		{"bad document: missing colon", `{a: 1, 'b'}`, nil, true},

		// list of expressions
		{"list with parentheses: empty", "()", query.LiteralExprList(nil), false},
		{"list with parentheses: values", `(1, true, {a: 1}, a.b.c, (-1), [-1])`,
			query.LiteralExprList{
				query.Int8Value(1),
				query.BoolValue(true),
				query.KVPairs{query.KVPair{K: "a", V: query.Int8Value(1)}},
				query.FieldSelector{"a", "b", "c"},
				query.LiteralExprList{query.Int8Value(-1)},
				query.LiteralExprList{query.Int8Value(-1)},
			}, false},
		{"list with parentheses: missing parenthese", `(1, true, {a: 1}, a.b.c, (-1)`, nil, true},
		{"list with brackets: empty", "[]", query.LiteralExprList(nil), false},
		{"list with brackets: values", `[1, true, {a: 1}, a.b.c, (-1), [-1]]`,
			query.LiteralExprList{
				query.Int8Value(1),
				query.BoolValue(true),
				query.KVPairs{query.KVPair{K: "a", V: query.Int8Value(1)}},
				query.FieldSelector{"a", "b", "c"},
				query.LiteralExprList{query.Int8Value(-1)},
				query.LiteralExprList{query.Int8Value(-1)},
			}, false},
		{"list with brackets: missing bracket", `[1, true, {a: 1}, a.b.c, (-1), [-1]`, nil, true},

		// operators
		{"=", "age = 10", query.Eq(query.FieldSelector([]string{"age"}), query.Int8Value(10)), false},
		{"!=", "age != 10", query.Neq(query.FieldSelector([]string{"age"}), query.Int8Value(10)), false},
		{">", "age > 10", query.Gt(query.FieldSelector([]string{"age"}), query.Int8Value(10)), false},
		{">=", "age >= 10", query.Gte(query.FieldSelector([]string{"age"}), query.Int8Value(10)), false},
		{"<", "age < 10", query.Lt(query.FieldSelector([]string{"age"}), query.Int8Value(10)), false},
		{"<=", "age <= 10", query.Lte(query.FieldSelector([]string{"age"}), query.Int8Value(10)), false},
		{"AND", "age = 10 AND age <= 11",
			query.And(
				query.Eq(query.FieldSelector([]string{"age"}), query.Int8Value(10)),
				query.Lte(query.FieldSelector([]string{"age"}), query.Int8Value(11)),
			), false},
		{"OR", "age = 10 OR age = 11",
			query.Or(
				query.Eq(query.FieldSelector([]string{"age"}), query.Int8Value(10)),
				query.Eq(query.FieldSelector([]string{"age"}), query.Int8Value(11)),
			), false},
		{"AND then OR", "age >= 10 AND age > $age OR age < 10.4",
			query.Or(
				query.And(
					query.Gte(query.FieldSelector([]string{"age"}), query.Int8Value(10)),
					query.Gt(query.FieldSelector([]string{"age"}), query.NamedParam("age")),
				),
				query.Lt(query.FieldSelector([]string{"age"}), query.Float64Value(10.4)),
			), false},
		{"with NULL", "age > NULL", query.Gt(query.FieldSelector([]string{"age"}), query.NullValue()), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ex, lit, err := NewParser(strings.NewReader(test.s)).parseExpr()
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, test.expected, ex)
				require.Equal(t, test.s, lit)
			}
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
		{"one positional", "age = ?", query.Eq(query.FieldSelector([]string{"age"}), query.PositionalParam(1)), false},
		{"multiple positional", "age = ? AND age <= ?",
			query.And(
				query.Eq(query.FieldSelector([]string{"age"}), query.PositionalParam(1)),
				query.Lte(query.FieldSelector([]string{"age"}), query.PositionalParam(2)),
			), false},
		{"one named", "age = $age", query.Eq(query.FieldSelector([]string{"age"}), query.NamedParam("age")), false},
		{"multiple named", "age = $foo OR age = $bar",
			query.Or(
				query.Eq(query.FieldSelector([]string{"age"}), query.NamedParam("foo")),
				query.Eq(query.FieldSelector([]string{"age"}), query.NamedParam("bar")),
			), false},
		{"mixed", "age >= ? AND age > $foo OR age < ?", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ex, lit, err := NewParser(strings.NewReader(test.s)).parseExpr()
			if test.errored {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, test.expected, ex)
				require.Equal(t, test.s, lit)
			}
		})
	}
}
