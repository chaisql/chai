package genji

import (
	"testing"

	"github.com/asdine/genji/engine/memory"
	"github.com/stretchr/testify/require"
)

func TestParserDrop(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected statement
		errored  bool
	}{
		{"Drop table", "DROP TABLE test", dropTableStmt{tableName: "test"}, false},
		{"Drop table If not exists", "DROP TABLE IF EXISTS test", dropTableStmt{tableName: "test", ifExists: true}, false},
		{"Drop index", "DROP INDEX test", dropIndexStmt{indexName: "test"}, false},
		{"Drop index if exists", "DROP INDEX IF EXISTS test", dropIndexStmt{indexName: "test", ifExists: true}, false},
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

func TestDropStmt(t *testing.T) {
	tests := []struct {
		name  string
		query string
		fails bool
	}{
		{"Drop table", "DROP TABLE test", false},
		{"Drop table If not exists", "DROP TABLE IF EXISTS test", false},
		{"Drop index", "DROP INDEX idx", false},
		{"Drop index if exists", "DROP INDEX IF EXISTS idx", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := New(memory.NewEngine())
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec("CREATE TABLE test; CREATE INDEX idx ON test (foo)")
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
