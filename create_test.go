package genji

import (
	"testing"

	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/value"
	"github.com/stretchr/testify/require"
)

func TestParserCreateTable(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected statement
		errored  bool
	}{
		{"Basic", "CREATE TABLE test", createTableStmt{tableName: "test"}, false},
		{"If not exists", "CREATE TABLE IF NOT EXISTS test", createTableStmt{tableName: "test", ifNotExists: true}, false},
		{"With primary key", "CREATE TABLE test(foo INT PRIMARY KEY)", createTableStmt{tableName: "test", primaryKeyName: "foo", primaryKeyType: value.Int}, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parseQuery(test.s)
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

func TestCreateTableStmt(t *testing.T) {
	tests := []struct {
		name  string
		query string
		fails bool
	}{
		{"Basic", `CREATE TABLE test`, false},
		{"Exists", "CREATE TABLE test;CREATE TABLE test", true},
		{"If not exists", "CREATE TABLE IF NOT EXISTS test", false},
		{"If not exists, twice", "CREATE TABLE IF NOT EXISTS test;CREATE TABLE IF NOT EXISTS test", false},
		{"With primary key", "CREATE TABLE test WITH PRIMARY KEY foo", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := New(memory.NewEngine())
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec(test.query)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			err = db.ViewTable("test", func(_ *Tx, _ *Table) error {
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func TestParserCreateIndex(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected statement
		errored  bool
	}{
		{"Basic", "CREATE INDEX idx ON test (foo)", createIndexStmt{indexName: "idx", tableName: "test", fieldName: "foo"}, false},
		{"If not exists", "CREATE INDEX IF NOT EXISTS idx ON test (foo)", createIndexStmt{indexName: "idx", tableName: "test", fieldName: "foo", ifNotExists: true}, false},
		{"Unique", "CREATE UNIQUE INDEX IF NOT EXISTS idx ON test (foo)", createIndexStmt{indexName: "idx", tableName: "test", fieldName: "foo", ifNotExists: true, unique: true}, false},
		{"No fields", "CREATE INDEX idx ON test", nil, true},
		{"More than 1 field", "CREATE INDEX idx ON test (foo, bar)", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parseQuery(test.s)
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

func TestCreateIndexStmt(t *testing.T) {
	tests := []struct {
		name  string
		query string
		fails bool
	}{
		{"Basic", "CREATE INDEX idx ON test (foo)", false},
		{"If not exists", "CREATE INDEX IF NOT EXISTS idx ON test (foo)", false},
		{"Unique", "CREATE UNIQUE INDEX IF NOT EXISTS idx ON test (foo)", false},
		{"No fields", "CREATE INDEX idx ON test", true},
		{"More than 1 field", "CREATE INDEX idx ON test (foo, bar)", true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := New(memory.NewEngine())
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec("CREATE TABLE test")
			require.NoError(t, err)

			err = db.Exec(test.query)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
