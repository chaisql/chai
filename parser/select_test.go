package parser

import (
	"testing"

	"github.com/asdine/genji/query"
	"github.com/stretchr/testify/require"
)

func TestParserSelect(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected query.Statement
		mustFail bool
	}{
		{"NoCond", "SELECT * FROM test",
			query.SelectStmt{
				Selectors: []query.ResultField{query.Wildcard{}},
				TableName: "test",
			}, false},
		{"WithFields", "SELECT a, b FROM test",
			query.SelectStmt{
				Selectors: []query.ResultField{query.FieldSelector("a"), query.FieldSelector("b")},
				TableName: "test",
			}, false},
		{"WithFields and wildcard", "SELECT a, b, * FROM test",
			query.SelectStmt{
				Selectors: []query.ResultField{query.FieldSelector("a"), query.FieldSelector("b"), query.Wildcard{}},
				TableName: "test",
			}, false},
		{"WithCond", "SELECT * FROM test WHERE age = 10",
			query.SelectStmt{
				TableName: "test",
				Selectors: []query.ResultField{query.Wildcard{}},
				WhereExpr: query.Eq(query.FieldSelector("age"), query.Int8Value(10)),
			}, false},
		{"WithLimit", "SELECT * FROM test WHERE age = 10 LIMIT 20",
			query.SelectStmt{
				Selectors: []query.ResultField{query.Wildcard{}},
				TableName: "test",
				WhereExpr: query.Eq(query.FieldSelector("age"), query.Int8Value(10)),
				LimitExpr: query.Int8Value(20),
			}, false},
		{"WithOffset", "SELECT * FROM test WHERE age = 10 OFFSET 20",
			query.SelectStmt{
				Selectors:  []query.ResultField{query.Wildcard{}},
				TableName:  "test",
				WhereExpr:  query.Eq(query.FieldSelector("age"), query.Int8Value(10)),
				OffsetExpr: query.Int8Value(20),
			}, false},
		{"WithLimitThenOffset", "SELECT * FROM test WHERE age = 10 LIMIT 10 OFFSET 20",
			query.SelectStmt{
				Selectors:  []query.ResultField{query.Wildcard{}},
				TableName:  "test",
				WhereExpr:  query.Eq(query.FieldSelector("age"), query.Int8Value(10)),
				OffsetExpr: query.Int8Value(20),
				LimitExpr:  query.Int8Value(10),
			}, false},
		{"WithOffsetThenLimit", "SELECT * FROM test WHERE age = 10 OFFSET 20 LIMIT 10", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := ParseQuery(test.s)
			if !test.mustFail {
				require.NoError(t, err)
				require.Len(t, q.Statements, 1)
				require.EqualValues(t, test.expected, q.Statements[0])
			} else {
				require.Error(t, err)
			}
		})
	}
}
