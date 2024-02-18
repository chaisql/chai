package parser_test

import (
	"context"
	"testing"

	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/query"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/stream"
	"github.com/chaisql/chai/internal/stream/path"
	"github.com/chaisql/chai/internal/stream/rows"
	"github.com/chaisql/chai/internal/stream/table"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestParserInsert(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected *stream.Stream
		fails    bool
	}{
		{"Values / With fields", "INSERT INTO test (a, b) VALUES ('c', 'd')",
			stream.New(rows.Emit(
				[]string{"a", "b"},
				expr.Row{
					Columns: []string{"a", "b"},
					Exprs: []expr.Expr{
						testutil.TextValue("c"),
						testutil.TextValue("d"),
					},
				},
			)).
				Pipe(table.Validate("test")).
				Pipe(table.Insert("test")).
				Pipe(stream.Discard()),
			false},
		{"Values / With too many values", "INSERT INTO test (a, b) VALUES ('c', 'd', 'e')",
			nil, true},
		{"Values / Multiple", "INSERT INTO test (a, b) VALUES ('c', 'd'), ('e', 'f')",
			stream.New(rows.Emit(
				[]string{"a", "b"},
				expr.Row{
					Columns: []string{"a", "b"},
					Exprs: []expr.Expr{
						testutil.TextValue("c"),
						testutil.TextValue("d"),
					},
				},
				expr.Row{
					Columns: []string{"a", "b"},
					Exprs: []expr.Expr{
						testutil.TextValue("e"),
						testutil.TextValue("f"),
					},
				},
			)).
				Pipe(table.Validate("test")).
				Pipe(table.Insert("test")).
				Pipe(stream.Discard()),
			false},
		{"Values / Returning", "INSERT INTO test (a, b) VALUES ('c', 'd') RETURNING *, a, b as B",
			stream.New(rows.Emit(
				[]string{"a", "b"},
				expr.Row{
					Columns: []string{"a", "b"},
					Exprs: []expr.Expr{
						testutil.TextValue("c"),
						testutil.TextValue("d"),
					},
				},
			)).
				Pipe(table.Validate("test")).
				Pipe(table.Insert("test")).
				Pipe(rows.Project(expr.Wildcard{}, testutil.ParseNamedExpr(t, "a"), testutil.ParseNamedExpr(t, "b", "B"))),
			false},
		{"Values / With fields / Wrong values", "INSERT INTO test (a, b) VALUES {a: 1}, ('e', 'f')",
			nil, true},
		{"Values / Without fields / Wrong values", "INSERT INTO test VALUES {a: 1}, ('e', 'f')",
			nil, true},
		{"Values / ON CONFLICT DO NOTHING", "INSERT INTO test (a, b) VALUES ('c', 'd') ON CONFLICT DO NOTHING RETURNING *",
			stream.New(rows.Emit(
				[]string{"a", "b"},
				expr.Row{
					Columns: []string{"a", "b"},
					Exprs: []expr.Expr{
						testutil.TextValue("c"),
						testutil.TextValue("d"),
					},
				},
			)).
				Pipe(table.Validate("test")).
				Pipe(stream.OnConflict(nil)).
				Pipe(table.Insert("test")).
				Pipe(rows.Project(expr.Wildcard{})),
			false},
		{"Values / ON CONFLICT IGNORE", "INSERT INTO test (a, b) VALUES ('c', 'd') ON CONFLICT IGNORE RETURNING *",
			stream.New(rows.Emit(
				[]string{"a", "b"},
				expr.Row{
					Columns: []string{"a", "b"},
					Exprs: []expr.Expr{
						testutil.TextValue("c"),
						testutil.TextValue("d"),
					},
				},
			)).Pipe(table.Validate("test")).
				Pipe(stream.OnConflict(nil)).
				Pipe(table.Insert("test")).
				Pipe(rows.Project(expr.Wildcard{})),
			false},
		{"Values / ON CONFLICT DO REPLACE", "INSERT INTO test (a, b) VALUES ('c', 'd') ON CONFLICT DO REPLACE RETURNING *",
			stream.New(rows.Emit(
				[]string{"a", "b"},
				expr.Row{
					Columns: []string{"a", "b"},
					Exprs: []expr.Expr{
						testutil.TextValue("c"),
						testutil.TextValue("d"),
					},
				},
			)).
				Pipe(table.Validate("test")).
				Pipe(stream.OnConflict(stream.New(table.Replace("test")))).
				Pipe(table.Insert("test")).
				Pipe(rows.Project(expr.Wildcard{})),
			false},
		{"Values / ON CONFLICT REPLACE", "INSERT INTO test (a, b) VALUES ('c', 'd') ON CONFLICT REPLACE RETURNING *",
			stream.New(rows.Emit(
				[]string{"a", "b"},
				expr.Row{
					Columns: []string{"a", "b"},
					Exprs: []expr.Expr{
						testutil.TextValue("c"),
						testutil.TextValue("d"),
					},
				},
			)).
				Pipe(table.Validate("test")).
				Pipe(stream.OnConflict(stream.New(table.Replace("test")))).
				Pipe(table.Insert("test")).
				Pipe(rows.Project(expr.Wildcard{})),
			false},
		{"Values / ON CONFLICT BLA", "INSERT INTO test (a, b) VALUES ('c', 'd') ON CONFLICT BLA RETURNING *",
			nil, true},
		{"Values / ON CONFLICT DO BLA", "INSERT INTO test (a, b) VALUES ('c', 'd') ON CONFLICT DO BLA RETURNING *",
			nil, true},
		{"Select / Without fields", "INSERT INTO test SELECT * FROM foo",
			stream.New(table.Scan("foo")).
				Pipe(rows.Project(expr.Wildcard{})).
				Pipe(table.Validate("test")).
				Pipe(table.Insert("test")).
				Pipe(stream.Discard()),
			false},
		{"Select / Without fields / With projection", "INSERT INTO test SELECT c, d FROM foo",
			stream.New(table.Scan("foo")).
				Pipe(rows.Project(testutil.ParseNamedExpr(t, "c"), testutil.ParseNamedExpr(t, "d"))).
				Pipe(table.Validate("test")).
				Pipe(table.Insert("test")).
				Pipe(stream.Discard()),
			false},
		{"Select / With fields", "INSERT INTO test (a, b) SELECT * FROM foo",
			stream.New(table.Scan("foo")).
				Pipe(rows.Project(expr.Wildcard{})).
				Pipe(path.PathsRename("a", "b")).
				Pipe(table.Validate("test")).
				Pipe(table.Insert("test")).
				Pipe(stream.Discard()),
			false},
		{"Select / With fields / With projection", "INSERT INTO test (a, b) SELECT c, d FROM foo",
			stream.New(table.Scan("foo")).
				Pipe(rows.Project(testutil.ParseNamedExpr(t, "c"), testutil.ParseNamedExpr(t, "d"))).
				Pipe(path.PathsRename("a", "b")).
				Pipe(table.Validate("test")).
				Pipe(table.Insert("test")).
				Pipe(stream.Discard()),
			false},
		{"Select / With fields / With projection / different fields", "INSERT INTO test (a, b) SELECT c, d FROM foo",
			stream.New(table.Scan("foo")).
				Pipe(rows.Project(testutil.ParseNamedExpr(t, "c"), testutil.ParseNamedExpr(t, "d"))).
				Pipe(path.PathsRename("a", "b")).
				Pipe(table.Validate("test")).
				Pipe(table.Insert("test")).
				Pipe(stream.Discard()),
			false},
		{"Select / With fields / With projection / different fields / Returning", "INSERT INTO test (a, b) SELECT c, d FROM foo RETURNING a",
			stream.New(table.Scan("foo")).
				Pipe(rows.Project(testutil.ParseNamedExpr(t, "c"), testutil.ParseNamedExpr(t, "d"))).
				Pipe(path.PathsRename("a", "b")).
				Pipe(table.Validate("test")).
				Pipe(table.Insert("test")).
				Pipe(rows.Project(testutil.ParseNamedExpr(t, "a"))),
			false},
		{"Select / With fields / With projection / different fields / On conflict / Returning", "INSERT INTO test (a, b) SELECT c, d FROM foo ON CONFLICT DO NOTHING RETURNING a",
			stream.New(table.Scan("foo")).
				Pipe(rows.Project(testutil.ParseNamedExpr(t, "c"), testutil.ParseNamedExpr(t, "d"))).
				Pipe(path.PathsRename("a", "b")).
				Pipe(table.Validate("test")).
				Pipe(stream.OnConflict(nil)).
				Pipe(table.Insert("test")).
				Pipe(rows.Project(testutil.ParseNamedExpr(t, "a"))),
			false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, "CREATE TABLE test(a TEXT, b TEXT); CREATE TABLE foo(c TEXT, d TEXT);")

			q, err := parser.ParseQuery(test.s)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			err = q.Prepare(&query.Context{
				Ctx:  context.Background(),
				DB:   db,
				Conn: tx.Connection(),
			})
			require.NoError(t, err)

			require.Len(t, q.Statements, 1)

			require.Equal(t, test.expected.String(), q.Statements[0].(*statement.PreparedStreamStmt).Stream.String())
		})
	}
}
