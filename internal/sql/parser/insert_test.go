package parser_test

import (
	"context"
	"testing"

	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/query"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
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
			stream.New(stream.DocsEmit(
				&expr.KVPairs{SelfReferenced: true, Pairs: []expr.KVPair{
					{K: "a", V: testutil.IntegerValue(1)},
					{K: "b", V: testutil.TextValue("foo")},
					{K: "c", V: testutil.TextValue("bar")},
					{K: "d", V: testutil.BoolValue(true)},
					{K: "e", V: &expr.KVPairs{SelfReferenced: true, Pairs: []expr.KVPair{
						{K: "f", V: testutil.TextValue("baz")},
					}}},
				}},
			)).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.TableInsert("test")),
			false},
		{"Documents / Multiple", `INSERT INTO test VALUES {"a": 'a', b: -2.3}, {a: 1, d: true}`,
			stream.New(stream.DocsEmit(
				&expr.KVPairs{SelfReferenced: true, Pairs: []expr.KVPair{
					{K: "a", V: testutil.TextValue("a")},
					{K: "b", V: testutil.DoubleValue(-2.3)},
				}},
				&expr.KVPairs{SelfReferenced: true, Pairs: []expr.KVPair{{K: "a", V: testutil.IntegerValue(1)}, {K: "d", V: testutil.BoolValue(true)}}},
			)).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.TableInsert("test")),
			false},
		{"Documents / Positional Param", "INSERT INTO test VALUES ?, ?",
			stream.New(stream.DocsEmit(
				expr.PositionalParam(1),
				expr.PositionalParam(2),
			)).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.TableInsert("test")),
			false},
		{"Documents / Named Param", "INSERT INTO test VALUES $foo, $bar",
			stream.New(stream.DocsEmit(
				expr.NamedParam("foo"),
				expr.NamedParam("bar"),
			)).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.TableInsert("test")),
			false},
		{"Values / With fields", "INSERT INTO test (a, b) VALUES ('c', 'd')",
			stream.New(stream.DocsEmit(
				&expr.KVPairs{Pairs: []expr.KVPair{
					{K: "a", V: testutil.TextValue("c")},
					{K: "b", V: testutil.TextValue("d")},
				}},
			)).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.TableInsert("test")),
			false},
		{"Values / With too many values", "INSERT INTO test (a, b) VALUES ('c', 'd', 'e')",
			nil, true},
		{"Values / Multiple", "INSERT INTO test (a, b) VALUES ('c', 'd'), ('e', 'f')",
			stream.New(stream.DocsEmit(
				&expr.KVPairs{Pairs: []expr.KVPair{
					{K: "a", V: testutil.TextValue("c")},
					{K: "b", V: testutil.TextValue("d")},
				}},
				&expr.KVPairs{Pairs: []expr.KVPair{
					{K: "a", V: testutil.TextValue("e")},
					{K: "b", V: testutil.TextValue("f")},
				}},
			)).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.TableInsert("test")),
			false},
		{"Values / Returning", "INSERT INTO test (a, b) VALUES ('c', 'd') RETURNING *, a, b as B, c",
			stream.New(stream.DocsEmit(
				&expr.KVPairs{Pairs: []expr.KVPair{
					{K: "a", V: testutil.TextValue("c")},
					{K: "b", V: testutil.TextValue("d")},
				}},
			)).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.TableInsert("test")).
				Pipe(stream.DocsProject(expr.Wildcard{}, testutil.ParseNamedExpr(t, "a"), testutil.ParseNamedExpr(t, "b", "B"), testutil.ParseNamedExpr(t, "c"))),
			false},
		{"Values / With fields / Wrong values", "INSERT INTO test (a, b) VALUES {a: 1}, ('e', 'f')",
			nil, true},
		{"Values / Without fields / Wrong values", "INSERT INTO test VALUES {a: 1}, ('e', 'f')",
			nil, true},
		{"Values / ON CONFLICT DO NOTHING", "INSERT INTO test (a, b) VALUES ('c', 'd') ON CONFLICT DO NOTHING RETURNING *",
			stream.New(stream.DocsEmit(
				&expr.KVPairs{Pairs: []expr.KVPair{
					{K: "a", V: testutil.TextValue("c")},
					{K: "b", V: testutil.TextValue("d")},
				}},
			)).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.OnConflict(nil)).
				Pipe(stream.TableInsert("test")),
			false},
		{"Values / ON CONFLICT IGNORE", "INSERT INTO test (a, b) VALUES ('c', 'd') ON CONFLICT IGNORE RETURNING *",
			stream.New(stream.DocsEmit(
				&expr.KVPairs{Pairs: []expr.KVPair{
					{K: "a", V: testutil.TextValue("c")},
					{K: "b", V: testutil.TextValue("d")},
				}},
			)).Pipe(stream.TableValidate("test")).
				Pipe(stream.OnConflict(nil)).
				Pipe(stream.TableInsert("test")),
			false},
		{"Values / ON CONFLICT DO REPLACE", "INSERT INTO test (a, b) VALUES ('c', 'd') ON CONFLICT DO REPLACE RETURNING *",
			stream.New(stream.DocsEmit(
				&expr.KVPairs{Pairs: []expr.KVPair{
					{K: "a", V: testutil.TextValue("c")},
					{K: "b", V: testutil.TextValue("d")},
				}},
			)).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.OnConflict(stream.New(stream.TableReplace("test")))).
				Pipe(stream.TableInsert("test")),
			false},
		{"Values / ON CONFLICT REPLACE", "INSERT INTO test (a, b) VALUES ('c', 'd') ON CONFLICT REPLACE RETURNING *",
			stream.New(stream.DocsEmit(
				&expr.KVPairs{Pairs: []expr.KVPair{
					{K: "a", V: testutil.TextValue("c")},
					{K: "b", V: testutil.TextValue("d")},
				}},
			)).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.OnConflict(stream.New(stream.TableReplace("test")))).
				Pipe(stream.TableInsert("test")),
			false},
		{"Values / ON CONFLICT BLA", "INSERT INTO test (a, b) VALUES ('c', 'd') ON CONFLICT BLA RETURNING *",
			nil, true},
		{"Values / ON CONFLICT DO BLA", "INSERT INTO test (a, b) VALUES ('c', 'd') ON CONFLICT DO BLA RETURNING *",
			nil, true},
		{"Select / Without fields", "INSERT INTO test SELECT * FROM foo",
			stream.New(stream.TableScan("foo")).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.TableInsert("test")),
			false},
		{"Select / Without fields / With projection", "INSERT INTO test SELECT a, b FROM foo",
			stream.New(stream.TableScan("foo")).
				Pipe(stream.DocsProject(testutil.ParseNamedExpr(t, "a"), testutil.ParseNamedExpr(t, "b"))).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.TableInsert("test")),
			false},
		{"Select / With fields", "INSERT INTO test (a, b) SELECT * FROM foo",
			stream.New(stream.TableScan("foo")).
				Pipe(stream.PathsRename("a", "b")).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.TableInsert("test")),
			false},
		{"Select / With fields / With projection", "INSERT INTO test (a, b) SELECT a, b FROM foo",
			stream.New(stream.TableScan("foo")).
				Pipe(stream.DocsProject(testutil.ParseNamedExpr(t, "a"), testutil.ParseNamedExpr(t, "b"))).
				Pipe(stream.PathsRename("a", "b")).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.TableInsert("test")),
			false},
		{"Select / With fields / With projection / different fields", "INSERT INTO test (a, b) SELECT c, d FROM foo",
			stream.New(stream.TableScan("foo")).
				Pipe(stream.DocsProject(testutil.ParseNamedExpr(t, "c"), testutil.ParseNamedExpr(t, "d"))).
				Pipe(stream.PathsRename("a", "b")).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.TableInsert("test")),
			false},
		{"Select / With fields / With projection / different fields / Returning", "INSERT INTO test (a, b) SELECT c, d FROM foo RETURNING a",
			stream.New(stream.TableScan("foo")).
				Pipe(stream.DocsProject(testutil.ParseNamedExpr(t, "c"), testutil.ParseNamedExpr(t, "d"))).
				Pipe(stream.PathsRename("a", "b")).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.TableInsert("test")).
				Pipe(stream.DocsProject(testutil.ParseNamedExpr(t, "a"))),
			false},
		{"Select / With fields / With projection / different fields / On conflict / Returning", "INSERT INTO test (a, b) SELECT c, d FROM foo ON CONFLICT DO NOTHING RETURNING a",
			stream.New(stream.TableScan("foo")).
				Pipe(stream.DocsProject(testutil.ParseNamedExpr(t, "c"), testutil.ParseNamedExpr(t, "d"))).
				Pipe(stream.PathsRename("a", "b")).
				Pipe(stream.TableValidate("test")).
				Pipe(stream.OnConflict(nil)).
				Pipe(stream.TableInsert("test")).
				Pipe(stream.DocsProject(testutil.ParseNamedExpr(t, "a"))),
			false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := testutil.NewTestDB(t)

			testutil.MustExec(t, db, nil, "CREATE TABLE test; CREATE TABLE foo;")

			q, err := parser.ParseQuery(test.s)
			if test.fails {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			err = q.Prepare(&query.Context{
				Ctx: context.Background(),
				DB:  db,
			})
			assert.NoError(t, err)

			require.Len(t, q.Statements, 1)

			require.Equal(t, test.expected.String(), q.Statements[0].(*statement.PreparedStreamStmt).Stream.String())
		})
	}
}
