package parser

import (
	"testing"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query"
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
		{"With primary key", "CREATE TABLE test(foo INTEGER PRIMARY KEY)",
			query.CreateTableStmt{
				TableName: "test",
				Info: database.TableInfo{
					FieldConstraints: []database.FieldConstraint{
						{Path: newFieldRef(t, "foo"), Type: document.IntegerValue, IsPrimaryKey: true},
					},
				},
			}, false},
		{"With primary key twice", "CREATE TABLE test(foo PRIMARY KEY PRIMARY KEY)",
			query.CreateTableStmt{}, true},
		{"With type", "CREATE TABLE test(foo INTEGER)",
			query.CreateTableStmt{
				TableName: "test",
				Info: database.TableInfo{
					FieldConstraints: []database.FieldConstraint{
						{Path: newFieldRef(t, "foo"), Type: document.IntegerValue},
					},
				},
			}, false},
		{"With not null", "CREATE TABLE test(foo NOT NULL)",
			query.CreateTableStmt{
				TableName: "test",
				Info: database.TableInfo{
					FieldConstraints: []database.FieldConstraint{
						{Path: newFieldRef(t, "foo"), IsNotNull: true},
					},
				},
			}, false},
		{"With not null twice", "CREATE TABLE test(foo NOT NULL NOT NULL)",
			query.CreateTableStmt{}, true},
		{"With type and not null", "CREATE TABLE test(foo INTEGER NOT NULL)",
			query.CreateTableStmt{
				TableName: "test",
				Info: database.TableInfo{
					FieldConstraints: []database.FieldConstraint{
						{Path: newFieldRef(t, "foo"), Type: document.IntegerValue, IsNotNull: true},
					},
				},
			}, false},
		{"With not null and primary key", "CREATE TABLE test(foo INTEGER NOT NULL PRIMARY KEY)",
			query.CreateTableStmt{
				TableName: "test",
				Info: database.TableInfo{
					FieldConstraints: []database.FieldConstraint{
						{Path: newFieldRef(t, "foo"), Type: document.IntegerValue, IsPrimaryKey: true, IsNotNull: true},
					},
				},
			}, false},
		{"With primary key and not null", "CREATE TABLE test(foo INTEGER PRIMARY KEY NOT NULL)",
			query.CreateTableStmt{
				TableName: "test",
				Info: database.TableInfo{
					FieldConstraints: []database.FieldConstraint{
						{Path: newFieldRef(t, "foo"), Type: document.IntegerValue, IsPrimaryKey: true, IsNotNull: true},
					},
				},
			}, false},
		{"With multiple constraints", "CREATE TABLE test(foo INTEGER PRIMARY KEY, bar INTEGER NOT NULL, baz[4][1].bat TEXT)",
			query.CreateTableStmt{
				TableName: "test",
				Info: database.TableInfo{
					FieldConstraints: []database.FieldConstraint{
						{Path: newFieldRef(t, "foo"), Type: document.IntegerValue, IsPrimaryKey: true},
						{Path: newFieldRef(t, "bar"), Type: document.IntegerValue, IsNotNull: true},
						{Path: newFieldRef(t, "baz[4][1].bat"), Type: document.TextValue},
					},
				},
			}, false},
		{"With multiple primary keys", "CREATE TABLE test(foo PRIMARY KEY, bar PRIMARY KEY)",
			query.CreateTableStmt{}, true},
		{"With all supported fixed size data types",
			"CREATE TABLE test(d double, b bool)",
			query.CreateTableStmt{
				TableName: "test",
				Info: database.TableInfo{
					FieldConstraints: []database.FieldConstraint{
						{Path: newFieldRef(t, "d"), Type: document.DoubleValue},
						{Path: newFieldRef(t, "b"), Type: document.BoolValue},
					},
				},
			}, false},
		{"With all supported variable size data types",
			"CREATE TABLE test(i integer, du duration, b blob, byt bytes, t text, a array, d document)",
			query.CreateTableStmt{
				TableName: "test",
				Info: database.TableInfo{
					FieldConstraints: []database.FieldConstraint{
						{Path: newFieldRef(t, "i"), Type: document.IntegerValue},
						{Path: newFieldRef(t, "du"), Type: document.DurationValue},
						{Path: newFieldRef(t, "b"), Type: document.BlobValue},
						{Path: newFieldRef(t, "byt"), Type: document.BlobValue},
						{Path: newFieldRef(t, "t"), Type: document.TextValue},
						{Path: newFieldRef(t, "a"), Type: document.ArrayValue},
						{Path: newFieldRef(t, "d"), Type: document.DocumentValue},
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
		{"Basic", "CREATE INDEX idx ON test (foo)", query.CreateIndexStmt{IndexName: "idx", TableName: "test", Path: newFieldRef(t, "foo")}, false},
		{"If not exists", "CREATE INDEX IF NOT EXISTS idx ON test (foo.bar[1])", query.CreateIndexStmt{IndexName: "idx", TableName: "test", Path: newFieldRef(t, "foo.bar[1]"), IfNotExists: true}, false},
		{"Unique", "CREATE UNIQUE INDEX IF NOT EXISTS idx ON test (foo[3].baz)", query.CreateIndexStmt{IndexName: "idx", TableName: "test", Path: newFieldRef(t, "foo[3].baz"), IfNotExists: true, Unique: true}, false},
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
