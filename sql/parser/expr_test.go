package parser

import (
	"strings"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/expr"
	"github.com/stretchr/testify/require"
)

func parsePath(t testing.TB, p string) expr.Path {
	t.Helper()

	vp, err := ParsePath(p)
	require.NoError(t, err)
	return expr.Path(vp)
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
		{"empty document", `{}`, &expr.KVPairs{SelfReferenced: true}, false},
		{"document values", `{a: 1, b: 1.0, c: true, d: 'string', e: "string", f: {foo: 'bar'}, g: h.i.j, k: [1, 2, 3]}`,
			&expr.KVPairs{SelfReferenced: true, Pairs: []expr.KVPair{
				{K: "a", V: expr.IntegerValue(1)},
				{K: "b", V: expr.DoubleValue(1)},
				{K: "c", V: expr.BoolValue(true)},
				{K: "d", V: expr.TextValue("string")},
				{K: "e", V: expr.TextValue("string")},
				{K: "f", V: &expr.KVPairs{SelfReferenced: true, Pairs: []expr.KVPair{
					{K: "foo", V: expr.TextValue("bar")},
				}}},
				{K: "g", V: parsePath(t, "h.i.j")},
				{K: "k", V: expr.LiteralExprList{expr.IntegerValue(1), expr.IntegerValue(2), expr.IntegerValue(3)}},
			}},
			false},
		{"document keys", `{a: 1, "foo bar __&&))": 1, 'ola ': 1}`,
			&expr.KVPairs{SelfReferenced: true, Pairs: []expr.KVPair{
				{K: "a", V: expr.IntegerValue(1)},
				{K: "foo bar __&&))", V: expr.IntegerValue(1)},
				{K: "ola ", V: expr.IntegerValue(1)},
			}},
			false},
		{"bad document keys: same key", `{a: 1, a: 2, "a": 3}`, nil, true},
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
				&expr.KVPairs{SelfReferenced: true, Pairs: []expr.KVPair{{K: "a", V: expr.IntegerValue(1)}}},
				parsePath(t, "a.b.c"),
				expr.Parentheses{E: expr.IntegerValue(-1)},
				expr.LiteralExprList{expr.IntegerValue(-1)},
			}, false},
		{"list with brackets: missing bracket", `[1, true, {a: 1}, a.b.c, (-1), [-1]`, nil, true},

		// operators
		{"=", "age = 10", expr.Eq(parsePath(t, "age"), expr.IntegerValue(10)), false},
		{"!=", "age != 10", expr.Neq(parsePath(t, "age"), expr.IntegerValue(10)), false},
		{">", "age > 10", expr.Gt(parsePath(t, "age"), expr.IntegerValue(10)), false},
		{">=", "age >= 10", expr.Gte(parsePath(t, "age"), expr.IntegerValue(10)), false},
		{"<", "age < 10", expr.Lt(parsePath(t, "age"), expr.IntegerValue(10)), false},
		{"<=", "age <= 10", expr.Lte(parsePath(t, "age"), expr.IntegerValue(10)), false},
		{"BETWEEN", "1 BETWEEN 10 AND 11", expr.Between(expr.IntegerValue(10))(expr.IntegerValue(1), expr.IntegerValue(11)), false},
		{"+", "age + 10", expr.Add(parsePath(t, "age"), expr.IntegerValue(10)), false},
		{"-", "age - 10", expr.Sub(parsePath(t, "age"), expr.IntegerValue(10)), false},
		{"*", "age * 10", expr.Mul(parsePath(t, "age"), expr.IntegerValue(10)), false},
		{"/", "age / 10", expr.Div(parsePath(t, "age"), expr.IntegerValue(10)), false},
		{"%", "age % 10", expr.Mod(parsePath(t, "age"), expr.IntegerValue(10)), false},
		{"&", "age & 10", expr.BitwiseAnd(parsePath(t, "age"), expr.IntegerValue(10)), false},
		{"||", "name || 'foo'", expr.Concat(parsePath(t, "name"), expr.TextValue("foo")), false},
		{"IN", "age IN ages", expr.In(parsePath(t, "age"), parsePath(t, "ages")), false},
		{"NOT IN", "age NOT IN ages", expr.NotIn(parsePath(t, "age"), parsePath(t, "ages")), false},
		{"IS", "age IS NULL", expr.Is(parsePath(t, "age"), expr.NullValue()), false},
		{"IS NOT", "age IS NOT NULL", expr.IsNot(parsePath(t, "age"), expr.NullValue()), false},
		{"LIKE", "name LIKE 'foo'", expr.Like(parsePath(t, "name"), expr.TextValue("foo")), false},
		{"NOT LIKE", "name NOT LIKE 'foo'", expr.NotLike(parsePath(t, "name"), expr.TextValue("foo")), false},
		{"NOT =", "name NOT = 'foo'", nil, true},
		{"precedence", "4 > 1 + 2", expr.Gt(
			expr.IntegerValue(4),
			expr.Add(
				expr.IntegerValue(1),
				expr.IntegerValue(2),
			),
		), false},
		{"AND", "age = 10 AND age <= 11",
			expr.And(
				expr.Eq(parsePath(t, "age"), expr.IntegerValue(10)),
				expr.Lte(parsePath(t, "age"), expr.IntegerValue(11)),
			), false},
		{"OR", "age = 10 OR age = 11",
			expr.Or(
				expr.Eq(parsePath(t, "age"), expr.IntegerValue(10)),
				expr.Eq(parsePath(t, "age"), expr.IntegerValue(11)),
			), false},
		{"AND then OR", "age >= 10 AND age > $age OR age < 10.4",
			expr.Or(
				expr.And(
					expr.Gte(parsePath(t, "age"), expr.IntegerValue(10)),
					expr.Gt(parsePath(t, "age"), expr.NamedParam("age")),
				),
				expr.Lt(parsePath(t, "age"), expr.DoubleValue(10.4)),
			), false},
		{"with NULL", "age > NULL", expr.Gt(parsePath(t, "age"), expr.NullValue()), false},

		// unary operators
		{"CAST", "CAST(a.b[1][0] AS TEXT)", expr.CastFunc{Expr: parsePath(t, "a.b[1][0]"), CastAs: document.TextValue}, false},
		{"NOT", "NOT 10", expr.Not(expr.IntegerValue(10)), false},
		{"NOT", "NOT NOT", nil, true},
		{"NOT", "NOT NOT 10", expr.Not(expr.Not(expr.IntegerValue(10))), false},

		// functions
		{"pk() function", "pk()", &expr.PKFunc{}, false},
		{"count(expr) function", "count(a)", &expr.CountFunc{Expr: parsePath(t, "a")}, false},
		{"count(*) function", "count(*)", &expr.CountFunc{Wildcard: true}, false},
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

func TestParsePath(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected document.Path
		fails    bool
	}{
		{"one fragment", `a`, document.Path{
			document.PathFragment{FieldName: "a"},
		}, false},
		{"one fragment with quotes", "`    \"a\"`", document.Path{
			document.PathFragment{FieldName: "    \"a\""},
		}, false},
		{"multiple fragments", `a.b[100].c[1][2]`, document.Path{
			document.PathFragment{FieldName: "a"},
			document.PathFragment{FieldName: "b"},
			document.PathFragment{ArrayIndex: 100},
			document.PathFragment{FieldName: "c"},
			document.PathFragment{ArrayIndex: 1},
			document.PathFragment{ArrayIndex: 2},
		}, false},
		{"with quotes", "`some ident`.` with`[5].`  \"quotes`", document.Path{
			document.PathFragment{FieldName: "some ident"},
			document.PathFragment{FieldName: " with"},
			document.PathFragment{ArrayIndex: 5},
			document.PathFragment{FieldName: "  \"quotes"},
		}, false},
		{"negative index", `a.b[-100].c`, nil, true},
		{"with spaces", `a.  b[100].  c`, nil, true},
		{"starting with array", `[10].a`, nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			vp, err := ParsePath(test.s)
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
		{"one positional", "age = ?", expr.Eq(parsePath(t, "age"), expr.PositionalParam(1)), false},
		{"multiple positional", "age = ? AND age <= ?",
			expr.And(
				expr.Eq(parsePath(t, "age"), expr.PositionalParam(1)),
				expr.Lte(parsePath(t, "age"), expr.PositionalParam(2)),
			), false},
		{"one named", "age = $age", expr.Eq(parsePath(t, "age"), expr.NamedParam("age")), false},
		{"multiple named", "age = $foo OR age = $bar",
			expr.Or(
				expr.Eq(parsePath(t, "age"), expr.NamedParam("foo")),
				expr.Eq(parsePath(t, "age"), expr.NamedParam("bar")),
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
