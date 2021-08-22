package parser_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/testutil/assert"
)

func TestParserMultiStatement(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected []statement.Statement
	}{
		{"OnlyCommas", ";;;", nil},
		{"TrailingComma", "SELECT * FROM foo;;;DELETE FROM foo;", []statement.Statement{
			&statement.StreamStmt{
				Stream:   stream.New(stream.SeqScan("foo")).Pipe(stream.Project(expr.Wildcard{})),
				ReadOnly: true,
			},
			&statement.StreamStmt{
				Stream: stream.New(stream.SeqScan("foo")).Pipe(stream.TableDelete("foo")),
			},
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parser.ParseQuery(test.s)
			assert.NoError(t, err)
			require.EqualValues(t, test.expected, q.Statements)
		})
	}
}

func TestParserDivideByZero(t *testing.T) {
	// See https://github.com/genjidb/genji/issues/268
	require.NotPanics(t, func() {
		_, _ = parser.ParseQuery("SELECT * FROM t LIMIT 0 % .5")
	})
}
