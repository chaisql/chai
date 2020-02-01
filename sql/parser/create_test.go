package parser

import (
	"testing"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/sql/query"
	"github.com/stretchr/testify/require"
)

func TestParserCreateTable(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Statement
		errored  bool
	}{
		{"Basic", "CREATE TABLE test", query.CreateTableStmt{TableName: "test"}, false},
		{"If not exists", "CREATE TABLE IF NOT EXISTS test", query.CreateTableStmt{TableName: "test", IfNotExists: true}, false},
		{"With primary key", "CREATE TABLE test(foo INT PRIMARY KEY)",
			query.CreateTableStmt{
				TableName: "test",
				Config: database.TableConfig{
					PrimaryKey: database.FieldConstraint{Path: []string{"foo"}, Type: document.Int64Value},
				},
			}, false},
		{"With multiple constraints", "CREATE TABLE test(foo INT PRIMARY KEY, bar INT16, baz.4.1.bat STRING)",
			query.CreateTableStmt{
				TableName: "test",
				Config: database.TableConfig{
					PrimaryKey: database.FieldConstraint{Path: []string{"foo"}, Type: document.Int64Value},
					FieldConstraints: []database.FieldConstraint{
						{Path: []string{"bar"}, Type: document.Int16Value},
						{Path: []string{"baz", "4", "1", "bat"}, Type: document.TextValue},
					},
				},
			}, false},
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

func TestParserCreateIndex(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Statement
		errored  bool
	}{
		{"Basic", "CREATE INDEX idx ON test (foo)", query.CreateIndexStmt{IndexName: "idx", TableName: "test", Path: document.NewValuePath("foo")}, false},
		{"If not exists", "CREATE INDEX IF NOT EXISTS idx ON test (foo.bar.1)", query.CreateIndexStmt{IndexName: "idx", TableName: "test", Path: document.NewValuePath("foo.bar.1"), IfNotExists: true}, false},
		{"Unique", "CREATE UNIQUE INDEX IF NOT EXISTS idx ON test (foo.3.baz)", query.CreateIndexStmt{IndexName: "idx", TableName: "test", Path: document.NewValuePath("foo.3.baz"), IfNotExists: true, Unique: true}, false},
		{"No fields", "CREATE INDEX idx ON test", nil, true},
		{"More than 1 field", "CREATE INDEX idx ON test (foo, bar)", nil, true},
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
