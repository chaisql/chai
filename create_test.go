package genji

import (
	"testing"

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
		{"If not exists", "CREATE TABLE test IF NOT EXISTS", createTableStmt{tableName: "test", ifNotExists: true}, false},
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
