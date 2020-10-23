package parser

import (
	"strings"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/stretchr/testify/require"
)

func parseReference(t testing.TB, ref string) document.Reference {
	t.Helper()

	vp, err := ParseReference(ref)
	require.NoError(t, err)
	return vp
}

func TestParserExpr(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected expr.Expr
		fails    bool
	}{
		// integers
		{"+int8", "10", expr.IntegerValue(10), false},
		{"-int8", "-10", expr.IntegerValue(-10), false},
		{"+int16", "1000", expr.IntegerValue(1000), false},
		{"-int16", "-1000", expr.IntegerValue(-1000), false},
		{"+int32", "10000000", expr.IntegerValue(10000000), false},
		{"-int32", "-10000000", expr.IntegerValue(-10000000), false},
		{"+int64", "10000000000", expr.IntegerValue(10000000000), false},
		{"-int64", "-10000000000", expr.IntegerValue(-10000000000), false},
		{"> max int64 -> float64", "10000000000000000000", expr.DoubleValue(10000000000000000000), false},
		{"< min int64 -> float64", "-10000000000000000000", expr.DoubleValue(-10000000000000000000), false},
		{"very large int", "100000000000000000000000000000000000000000000000", expr.DoubleValue(100000000000000000000000000000000000000000000000), false},

		// floats
		{"+float64", "10.0", expr.DoubleValue(10), false},
		{"-float64", "-10.0", expr.DoubleValue(-10), false},

		// strings
		{"double quoted string", `"10.0"`, expr.TextValue("10.0"), false},
		{"single quoted string", "'-10.0'", expr.TextValue("-10.0"), false},

		// documents
		{"empty document", `{}`, expr.KVPairs(nil), false},
		{"document values", `{a: 1, b: 1.0, c: true, d: 'string', e: "string", f: {foo: 'bar'}, g: h.i.j, k: [1, 2, 3]}`,
			expr.KVPairs{
				expr.KVPair{K: "a", V: expr.IntegerValue(1)},
				expr.KVPair{K: "b", V: expr.DoubleValue(1)},
				expr.KVPair{K: "c", V: expr.BoolValue(true)},
				expr.KVPair{K: "d", V: expr.TextValue("string")},
				expr.KVPair{K: "e", V: expr.TextValue("string")},
				expr.KVPair{K: "f", V: expr.KVPairs{
					expr.KVPair{K: "foo", V: expr.TextValue("bar")},
				}},
				expr.KVPair{K: "g", V: expr.Reference(parseReference(t, "h.i.j"))},
				expr.KVPair{K: "k", V: expr.LiteralExprList{expr.IntegerValue(1), expr.IntegerValue(2), expr.IntegerValue(3)}},
			},
			false},
		{"document keys", `{a: 1, "foo bar __&&))": 1, 'ola ': 1}`,
			expr.KVPairs{
				expr.KVPair{K: "a", V: expr.IntegerValue(1)},
				expr.KVPair{K: "foo bar __&&))", V: expr.IntegerValue(1)},
				expr.KVPair{K: "ola ", V: expr.IntegerValue(1)},
			},
			false},
		{"document keys: same key", `{a: 1, a: 2, "a": 3}`,
			expr.KVPairs{
				expr.KVPair{K: "a", V: expr.IntegerValue(1)},
				expr.KVPair{K: "a", V: expr.IntegerValue(2)},
				expr.KVPair{K: "a", V: expr.IntegerValue(3)},
			},
			false},
		{"bad document keys: param", `{?: 1}`, nil, true},
		{"bad document keys: dot", `{a.b: 1}`, nil, true},
		{"bad document keys: space", `{a b: 1}`, nil, true},
		{"bad document: missing right bracket", `{a: 1`, nil, true},
		{"bad document: missing colon", `{a: 1, 'b'}`, nil, true},

		// parentheses
		{"parentheses: empty", "()", nil, true},
		{"parentheses: values", `(1)`,
			expr.Parentheses{
				E: expr.IntegerValue(1),
			}, false},
		{"parentheses: expr", `(1 + true * (4 + 3))`,
			expr.Parentheses{
				E: expr.Add(
					expr.IntegerValue(1),
					expr.Mul(
						expr.BoolValue(true),
						expr.Parentheses{
							E: expr.Add(
								expr.IntegerValue(4),
								expr.IntegerValue(3),
							),
						},
					),
				),
			}, false},
		{"list with brackets: empty", "[]", expr.LiteralExprList(nil), false},
		{"list with brackets: values", `[1, true, {a: 1}, a.b.c, (-1), [-1]]`,
			expr.LiteralExprList{
				expr.IntegerValue(1),
				expr.BoolValue(true),
				expr.KVPairs{expr.KVPair{K: "a", V: expr.IntegerValue(1)}},
				expr.Reference(parseReference(t, "a.b.c")),
				expr.Parentheses{E: expr.IntegerValue(-1)},
				expr.LiteralExprList{expr.IntegerValue(-1)},
			}, false},
		{"list with brackets: missing bracket", `[1, true, {a: 1}, a.b.c, (-1), [-1]`, nil, true},

		// operators
		{"=", "age = 10", expr.Eq(expr.Reference(parseReference(t, "age")), expr.IntegerValue(10)), false},
		{"!=", "age != 10", expr.Neq(expr.Reference(parseReference(t, "age")), expr.IntegerValue(10)), false},
		{">", "age > 10", expr.Gt(expr.Reference(parseReference(t, "age")), expr.IntegerValue(10)), false},
		{">=", "age >= 10", expr.Gte(expr.Reference(parseReference(t, "age")), expr.IntegerValue(10)), false},
		{"<", "age < 10", expr.Lt(expr.Reference(parseReference(t, "age")), expr.IntegerValue(10)), false},
		{"<=", "age <= 10", expr.Lte(expr.Reference(parseReference(t, "age")), expr.IntegerValue(10)), false},
		{"+", "age + 10", expr.Add(expr.Reference(parseReference(t, "age")), expr.IntegerValue(10)), false},
		{"-", "age - 10", expr.Sub(expr.Reference(parseReference(t, "age")), expr.IntegerValue(10)), false},
		{"*", "age * 10", expr.Mul(expr.Reference(parseReference(t, "age")), expr.IntegerValue(10)), false},
		{"/", "age / 10", expr.Div(expr.Reference(parseReference(t, "age")), expr.IntegerValue(10)), false},
		{"%", "age % 10", expr.Mod(expr.Reference(parseReference(t, "age")), expr.IntegerValue(10)), false},
		{"&", "age & 10", expr.BitwiseAnd(expr.Reference(parseReference(t, "age")), expr.IntegerValue(10)), false},
		{"IN", "age IN ages", expr.In(expr.Reference(parseReference(t, "age")), expr.Reference(parseReference(t, "ages"))), false},
		{"IS", "age IS NULL", expr.Is(expr.Reference(parseReference(t, "age")), expr.NullValue()), false},
		{"IS NOT", "age IS NOT NULL", expr.IsNot(expr.Reference(parseReference(t, "age")), expr.NullValue()), false},
		{"precedence", "4 > 1 + 2", expr.Gt(
			expr.IntegerValue(4),
			expr.Add(
				expr.IntegerValue(1),
				expr.IntegerValue(2),
			),
		), false},
		{"AND", "age = 10 AND age <= 11",
			expr.And(
				expr.Eq(expr.Reference(parseReference(t, "age")), expr.IntegerValue(10)),
				expr.Lte(expr.Reference(parseReference(t, "age")), expr.IntegerValue(11)),
			), false},
		{"OR", "age = 10 OR age = 11",
			expr.Or(
				expr.Eq(expr.Reference(parseReference(t, "age")), expr.IntegerValue(10)),
				expr.Eq(expr.Reference(parseReference(t, "age")), expr.IntegerValue(11)),
			), false},
		{"AND then OR", "age >= 10 AND age > $age OR age < 10.4",
			expr.Or(
				expr.And(
					expr.Gte(expr.Reference(parseReference(t, "age")), expr.IntegerValue(10)),
					expr.Gt(expr.Reference(parseReference(t, "age")), expr.NamedParam("age")),
				),
				expr.Lt(expr.Reference(parseReference(t, "age")), expr.DoubleValue(10.4)),
			), false},
		{"with NULL", "age > NULL", expr.Gt(expr.Reference(parseReference(t, "age")), expr.NullValue()), false},
		{"pk() function", "pk()", &expr.PKFunc{}, false},
		{"count(expr) function", "count(a)", &expr.CountFunc{Expr: expr.Reference(parseReference(t, "a"))}, false},
		{"count(*) function", "count(*)", &expr.CountFunc{Wildcard: true}, false},
		{"CAST", "CAST(a.b[1][0] AS TEXT)", expr.CastFunc{Expr: expr.Reference(parseReference(t, "a.b[1][0]")), CastAs: document.TextValue}, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ex, lit, err := NewParser(strings.NewReader(test.s)).ParseExpr()
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

func TestParseReference(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected document.Reference
		fails    bool
	}{
		{"one fragment", `a`, document.Reference{
			document.ReferenceFragment{FieldName: "a"},
		}, false},
		{"one fragment with quotes", "`    \"a\"`", document.Reference{
			document.ReferenceFragment{FieldName: "    \"a\""},
		}, false},
		{"multiple fragments", `a.b[100].c[1][2]`, document.Reference{
			document.ReferenceFragment{FieldName: "a"},
			document.ReferenceFragment{FieldName: "b"},
			document.ReferenceFragment{ArrayIndex: 100},
			document.ReferenceFragment{FieldName: "c"},
			document.ReferenceFragment{ArrayIndex: 1},
			document.ReferenceFragment{ArrayIndex: 2},
		}, false},
		{"with quotes", "`some ident`.` with`[5].`  \"quotes`", document.Reference{
			document.ReferenceFragment{FieldName: "some ident"},
			document.ReferenceFragment{FieldName: " with"},
			document.ReferenceFragment{ArrayIndex: 5},
			document.ReferenceFragment{FieldName: "  \"quotes"},
		}, false},
		{"negative index", `a.b[-100].c`, nil, true},
		{"with spaces", `a.  b[100].  c`, nil, true},
		{"starting with array", `[10].a`, nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			vp, err := ParseReference(test.s)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, test.expected, vp)
			}
		})
	}
}

func TestParserParams(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected expr.Expr
		errored  bool
	}{
		{"one positional", "age = ?", expr.Eq(expr.Reference(parseReference(t, "age")), expr.PositionalParam(1)), false},
		{"multiple positional", "age = ? AND age <= ?",
			expr.And(
				expr.Eq(expr.Reference(parseReference(t, "age")), expr.PositionalParam(1)),
				expr.Lte(expr.Reference(parseReference(t, "age")), expr.PositionalParam(2)),
			), false},
		{"one named", "age = $age", expr.Eq(expr.Reference(parseReference(t, "age")), expr.NamedParam("age")), false},
		{"multiple named", "age = $foo OR age = $bar",
			expr.Or(
				expr.Eq(expr.Reference(parseReference(t, "age")), expr.NamedParam("foo")),
				expr.Eq(expr.Reference(parseReference(t, "age")), expr.NamedParam("bar")),
			), false},
		{"mixed", "age >= ? AND age > $foo OR age < ?", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ex, lit, err := NewParser(strings.NewReader(test.s)).ParseExpr()
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
