package parser

import (
	"testing"

	"github.com/asdine/genji/query"
	"github.com/stretchr/testify/require"
)

func TestParserDropTable(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Statement
		errored  bool
	}{
		{"Basic", "DROP TABLE test", query.DropTable("test"), false},
		{"If not exists", "DROP TABLE IF EXISTS test", query.DropTable("test").IfExists(), false},
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
