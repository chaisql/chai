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
					FieldConstraints: []database.FieldConstraint{
						{Path: []string{"foo"}, Type: document.Int64Value, IsPrimaryKey: true},
					},
				},
			}, false},
		{"With primary key twice", "CREATE TABLE test(foo PRIMARY KEY PRIMARY KEY)",
			query.CreateTableStmt{}, true},
		{"With type", "CREATE TABLE test(foo INT)",
			query.CreateTableStmt{
				TableName: "test",
				Config: database.TableConfig{
					FieldConstraints: []database.FieldConstraint{
						{Path: []string{"foo"}, Type: document.Int64Value},
					},
				},
			}, false},
		{"With not null", "CREATE TABLE test(foo NOT NULL)",
			query.CreateTableStmt{
				TableName: "test",
				Config: database.TableConfig{
					FieldConstraints: []database.FieldConstraint{
						{Path: []string{"foo"}, IsNotNull: true},
					},
				},
			}, false},
		{"With not null twice", "CREATE TABLE test(foo NOT NULL NOT NULL)",
			query.CreateTableStmt{}, true},
		{"With type and not null", "CREATE TABLE test(foo INT NOT NULL)",
			query.CreateTableStmt{
				TableName: "test",
				Config: database.TableConfig{
					FieldConstraints: []database.FieldConstraint{
						{Path: []string{"foo"}, Type: document.Int64Value, IsNotNull: true},
					},
				},
			}, false},
		{"With not null and primary key", "CREATE TABLE test(foo INT NOT NULL PRIMARY KEY)",
			query.CreateTableStmt{
				TableName: "test",
				Config: database.TableConfig{
					FieldConstraints: []database.FieldConstraint{
						{Path: []string{"foo"}, Type: document.Int64Value, IsPrimaryKey: true, IsNotNull: true},
					},
				},
			}, false},
		{"With primary key and not null", "CREATE TABLE test(foo INT PRIMARY KEY NOT NULL)",
			query.CreateTableStmt{
				TableName: "test",
				Config: database.TableConfig{
					FieldConstraints: []database.FieldConstraint{
						{Path: []string{"foo"}, Type: document.Int64Value, IsPrimaryKey: true, IsNotNull: true},
					},
				},
			}, false},
		{"With multiple constraints", "CREATE TABLE test(foo INT PRIMARY KEY, bar INT16 NOT NULL, baz.4.1.bat STRING)",
			query.CreateTableStmt{
				TableName: "test",
				Config: database.TableConfig{
					FieldConstraints: []database.FieldConstraint{
						{Path: []string{"foo"}, Type: document.Int64Value, IsPrimaryKey: true},
						{Path: []string{"bar"}, Type: document.Int16Value, IsNotNull: true},
						{Path: []string{"baz", "4", "1", "bat"}, Type: document.TextValue},
					},
				},
			}, false},
		{"With multiple primary keys", "CREATE TABLE test(foo PRIMARY KEY, bar PRIMARY KEY)",
			query.CreateTableStmt{}, true},
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
