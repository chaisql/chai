package parser

import (
	"strings"
	"testing"

	"github.com/asdine/genji/query"
	"github.com/asdine/genji/query/expr"
	"github.com/asdine/genji/query/q"
	"github.com/stretchr/testify/require"
)

func TestParserExpr(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected expr.Expr
	}{
		{"=", "age = 10", expr.Eq(q.Field("age"), expr.Int64Value(10))},
		{"AND", "age = 10 AND age <= 11",
			expr.And(
				expr.Eq(q.Field("age"), expr.Int64Value(10)),
				expr.Lte(q.Field("age"), expr.Int64Value(11)),
			)},
		{"OR", "age = 10 OR age = 11",
			expr.Or(
				expr.Eq(q.Field("age"), expr.Int64Value(10)),
				expr.Eq(q.Field("age"), expr.Int64Value(11)),
			)},
		{"AND then OR", "age >= 10 AND age > $age OR age < 10.4",
			expr.Or(
				expr.And(
					expr.Gte(q.Field("age"), expr.Int64Value(10)),
					expr.Gt(q.Field("age"), expr.NamedParam("age")),
				),
				expr.Lt(q.Field("age"), expr.Float64Value(10.4)),
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

func TestParserParams(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected expr.Expr
		errored  bool
	}{
		{"one positional", "age = ?", expr.Eq(q.Field("age"), expr.PositionalParam(1)), false},
		{"multiple positional", "age = ? AND age <= ?",
			expr.And(
				expr.Eq(q.Field("age"), expr.PositionalParam(1)),
				expr.Lte(q.Field("age"), expr.PositionalParam(2)),
			), false},
		{"one named", "age = $age", expr.Eq(q.Field("age"), expr.NamedParam("age")), false},
		{"multiple named", "age = $foo OR age = $bar",
			expr.Or(
				expr.Eq(q.Field("age"), expr.NamedParam("foo")),
				expr.Eq(q.Field("age"), expr.NamedParam("bar")),
			), false},
		{"mixed", "age >= ? AND age > $foo OR age < ?", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ex, err := NewParser(strings.NewReader(test.s)).ParseExpr()
			if test.errored {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, test.expected, ex)
			}
		})
	}
}

func TestParserMultiStatement(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected []query.Statement
	}{
		{"OnlyCommas", ";;;", nil},
		{"TrailingComma", "SELECT * FROM foo;;;DELETE FROM foo;", []query.Statement{
			query.Select().From(q.Table("foo")),
			query.Delete().From(q.Table("foo")),
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := ParseQuery(test.s)
			require.NoError(t, err)
			require.EqualValues(t, test.expected, q.Statements)
		})
	}
}

func TestParserSelect(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Statement
	}{
		{"NoCond", "SELECT * FROM test", query.Select().From(q.Table("test"))},
		{"WithFields", "SELECT a, b FROM test", query.Select(q.Field("a"), q.Field("b")).From(q.Table("test"))},
		{"WithCond", "SELECT * FROM test WHERE age = 10", query.Select().From(q.Table("test")).Where(expr.Eq(q.Field("age"), expr.Int64Value(10)))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := ParseQuery(test.s)
			require.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}

func TestParserDelete(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Statement
	}{
		{"NoCond", "DELETE FROM test", query.Delete().From(q.Table("test"))},
		{"WithCond", "DELETE FROM test WHERE age = 10", query.Delete().From(q.Table("test")).Where(expr.Eq(q.Field("age"), expr.Int64Value(10)))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := ParseQuery(test.s)
			require.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}

func TestParserUdpate(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Statement
		errored  bool
	}{
		{"No cond", "UPDATE test SET a = 1", query.Update(q.Table("test")).Set("a", expr.Int64Value(1)), false},
		{"With cond", "UPDATE test SET a = 1, b = 2 WHERE age = 10", query.Update(q.Table("test")).Set("a", expr.Int64Value(1)).Set("b", expr.Int64Value(2)).Where(expr.Eq(q.Field("age"), expr.Int64Value(10))), false},
		{"Trailing comma", "UPDATE test SET a = 1, WHERE age = 10", nil, true},
		{"No SET", "UPDATE test WHERE age = 10", nil, true},
		{"No pair", "UPDATE test SET WHERE age = 10", nil, true},
		{"query.Field only", "UPDATE test SET a WHERE age = 10", nil, true},
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
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}

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

func TestParserCreateTable(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Statement
		errored  bool
	}{
		{"Basic", "CREATE TABLE test", query.CreateTable("test"), false},
		{"If not exists", "CREATE TABLE test IF NOT EXISTS", query.CreateTable("test").IfNotExists(), false},
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
