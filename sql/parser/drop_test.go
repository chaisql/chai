package parser

import (
	"testing"

	"github.com/asdine/genji/query"
	"github.com/stretchr/testify/require"
)

func TestParserDrop(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Statement
		errored  bool
	}{
		{"Drop table", "DROP TABLE test", query.DropTable("test"), false},
		{"Drop table If not exists", "DROP TABLE IF EXISTS test", query.DropTable("test").IfExists(), false},
		{"Drop index", "DROP INDEX test", query.DropIndex("test"), false},
		{"Drop index if exists", "DROP INDEX IF EXISTS test", query.DropIndex("test").IfExists(), false},
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
