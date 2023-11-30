package parser_test

import (
	"math"
	"testing"

	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/object"
	"github.com/stretchr/testify/require"
)

func TestParserCreateIndex(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected statement.Statement
		errored  bool
	}{
		{"Basic", "CREATE INDEX idx ON test (foo)", &statement.CreateIndexStmt{
			Info: database.IndexInfo{
				IndexName: "idx", Owner: database.Owner{TableName: "test"}, Paths: []object.Path{object.Path(testutil.ParseObjectPath(t, "foo"))},
			}}, false},
		{"If not exists", "CREATE INDEX IF NOT EXISTS idx ON test (foo.bar[1])", &statement.CreateIndexStmt{
			Info: database.IndexInfo{
				IndexName: "idx", Owner: database.Owner{TableName: "test"}, Paths: []object.Path{object.Path(testutil.ParseObjectPath(t, "foo.bar[1]"))},
			}, IfNotExists: true}, false},
		{"Unique", "CREATE UNIQUE INDEX IF NOT EXISTS idx ON test (foo[3].baz)", &statement.CreateIndexStmt{
			Info: database.IndexInfo{
				IndexName: "idx", Owner: database.Owner{TableName: "test"}, Paths: []object.Path{object.Path(testutil.ParseObjectPath(t, "foo[3].baz"))}, Unique: true,
			}, IfNotExists: true}, false},
		{"No name", "CREATE UNIQUE INDEX ON test (foo[3].baz)", &statement.CreateIndexStmt{
			Info: database.IndexInfo{Owner: database.Owner{TableName: "test"}, Paths: []object.Path{object.Path(testutil.ParseObjectPath(t, "foo[3].baz"))}, Unique: true}}, false},
		{"No name with IF NOT EXISTS", "CREATE UNIQUE INDEX IF NOT EXISTS ON test (foo[3].baz)", nil, true},
		{"More than 1 path", "CREATE INDEX idx ON test (foo, bar)",
			&statement.CreateIndexStmt{
				Info: database.IndexInfo{
					IndexName: "idx",
					Owner:     database.Owner{TableName: "test"},
					Paths: []object.Path{
						object.Path(testutil.ParseObjectPath(t, "foo")),
						object.Path(testutil.ParseObjectPath(t, "bar")),
					},
				},
			},
			false},
		{"No fields", "CREATE INDEX idx ON test", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parser.ParseQuery(test.s)
			if test.errored {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}

func TestParserCreateSequence(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected statement.Statement
		errored  bool
	}{
		{"Basic", "CREATE SEQUENCE seq", &statement.CreateSequenceStmt{
			Info: database.SequenceInfo{Name: "seq", IncrementBy: 1, Min: 1, Max: math.MaxInt64, Start: 1, Cache: 1},
		}, false},
		{"If not exists", "CREATE SEQUENCE IF NOT EXISTS seq", &statement.CreateSequenceStmt{
			Info:        database.SequenceInfo{Name: "seq", IncrementBy: 1, Min: 1, Max: math.MaxInt64, Start: 1, Cache: 1},
			IfNotExists: true,
		}, false},
		{"AS integer", "CREATE SEQUENCE seq AS TINYINT", &statement.CreateSequenceStmt{
			Info: database.SequenceInfo{Name: "seq", IncrementBy: 1, Min: 1, Max: math.MaxInt64, Start: 1, Cache: 1},
		}, false},
		{"AS double", "CREATE SEQUENCE seq AS DOUBLE", nil, true},
		{"INCREMENT", "CREATE SEQUENCE seq INCREMENT 10", &statement.CreateSequenceStmt{
			Info: database.SequenceInfo{Name: "seq", IncrementBy: 10, Min: 1, Max: math.MaxInt64, Start: 1, Cache: 1},
		}, false},
		{"INCREMENT BY", "CREATE SEQUENCE seq INCREMENT BY 10", &statement.CreateSequenceStmt{
			Info: database.SequenceInfo{Name: "seq", IncrementBy: 10, Min: 1, Max: math.MaxInt64, Start: 1, Cache: 1},
		}, false},
		{"INCREMENT BY 0", "CREATE SEQUENCE seq INCREMENT BY 0", nil, true},
		{"NO MINVALUE", "CREATE SEQUENCE seq NO MINVALUE", &statement.CreateSequenceStmt{
			Info: database.SequenceInfo{Name: "seq", IncrementBy: 1, Min: 1, Max: math.MaxInt64, Start: 1, Cache: 1},
		}, false},
		{"NO MAXVALUE", "CREATE SEQUENCE seq NO MAXVALUE", &statement.CreateSequenceStmt{
			Info: database.SequenceInfo{Name: "seq", IncrementBy: 1, Min: 1, Max: math.MaxInt64, Start: 1, Cache: 1},
		}, false},
		{"NO CYCLE", "CREATE SEQUENCE seq NO CYCLE", &statement.CreateSequenceStmt{
			Info: database.SequenceInfo{Name: "seq", IncrementBy: 1, Min: 1, Max: math.MaxInt64, Start: 1, Cache: 1},
		}, false},
		{"NO SUGAR", "CREATE SEQUENCE seq NO SUGAR", nil, true},
		{"MINVALUE 10", "CREATE SEQUENCE seq MINVALUE 10", &statement.CreateSequenceStmt{
			Info: database.SequenceInfo{Name: "seq", IncrementBy: 1, Min: 10, Max: math.MaxInt64, Start: 10, Cache: 1},
		}, false},
		{"MINVALUE 'hello'", "CREATE SEQUENCE seq MINVALUE 'hello'", nil, true},
		{"MAXVALUE 10", "CREATE SEQUENCE seq MAXVALUE 10", &statement.CreateSequenceStmt{
			Info: database.SequenceInfo{Name: "seq", IncrementBy: 1, Min: 1, Max: 10, Start: 1, Cache: 1},
		}, false},
		{"MAXVALUE 'hello'", "CREATE SEQUENCE seq MAXVALUE 'hello'", nil, true},
		{"START WITH 10", "CREATE SEQUENCE seq START WITH 10", &statement.CreateSequenceStmt{
			Info: database.SequenceInfo{Name: "seq", IncrementBy: 1, Min: 1, Max: math.MaxInt64, Start: 10, Cache: 1},
		}, false},
		{"START WITH 'hello'", "CREATE SEQUENCE seq START WITH 'hello'", nil, true},
		{"START 10", "CREATE SEQUENCE seq START 10", &statement.CreateSequenceStmt{
			Info: database.SequenceInfo{Name: "seq", IncrementBy: 1, Min: 1, Max: math.MaxInt64, Start: 10, Cache: 1},
		}, false},
		{"CACHE 10", "CREATE SEQUENCE seq CACHE 10", &statement.CreateSequenceStmt{
			Info: database.SequenceInfo{Name: "seq", IncrementBy: 1, Min: 1, Max: math.MaxInt64, Start: 1, Cache: 10},
		}, false},
		{"CACHE 'hello'", "CREATE SEQUENCE seq CACHE 'hello'", nil, true},
		{"CACHE -10", "CREATE SEQUENCE seq CACHE -10", nil, true},
		{"CYCLE", "CREATE SEQUENCE seq CYCLE", &statement.CreateSequenceStmt{
			Info: database.SequenceInfo{Name: "seq", IncrementBy: 1, Min: 1, Max: math.MaxInt64, Start: 1, Cache: 1, Cycle: true},
		}, false},
		{"Order 1", `
			CREATE SEQUENCE IF NOT EXISTS seq
			AS INTEGER
			INCREMENT BY 2
			NO MINVALUE
			MAXVALUE 10
			START WITH 5
			CACHE 5
			CYCLE
		`, &statement.CreateSequenceStmt{
			IfNotExists: true,
			Info: database.SequenceInfo{
				Name:        "seq",
				IncrementBy: 2,
				Min:         1,
				Max:         10,
				Start:       5,
				Cache:       5,
				Cycle:       true,
			},
		}, false},
		{"Order 2", `
			CREATE SEQUENCE IF NOT EXISTS seq
			CYCLE
			MAXVALUE 10
			INCREMENT BY 2
			START WITH 5
			AS INTEGER
			NO MINVALUE
			CACHE 5
		`, &statement.CreateSequenceStmt{
			IfNotExists: true,
			Info: database.SequenceInfo{
				Name:        "seq",
				IncrementBy: 2,
				Min:         1,
				Max:         10,
				Start:       5,
				Cache:       5,
				Cycle:       true,
			},
		}, false},
		{"NO MINVALUE with MINVALUE 10", "CREATE SEQUENCE seq NO MINVALUE MINVALUE 10", nil, true},
		{"NO MAXVALUE with MAXVALUE 10", "CREATE SEQUENCE seq NO MAXVALUE MAXVALUE 10", nil, true},
		{"NO CYCLE with CYCLE", "CREATE SEQUENCE seq NO MAXVALUE MAXVALUE 10", nil, true},
		{"duplicate AS INT", "CREATE SEQUENCE seq AS INT AS INT", nil, true},
		{"duplicate INCREMENT BY", "CREATE SEQUENCE seq INCREMENT BY 10 INCREMENT BY 10", nil, true},
		{"duplicate NO MINVALUE", "CREATE SEQUENCE seq NO MINVALUE NO MINVALUE", nil, true},
		{"duplicate NO MAXVALUE", "CREATE SEQUENCE seq NO MAXVALUE NO MAXVALUE", nil, true},
		{"duplicate NO CYCLE", "CREATE SEQUENCE seq NO CYCLE NO CYCLE", nil, true},
		{"duplicate MINVALUE", "CREATE SEQUENCE seq MINVALUE 10 MINVALUE 10", nil, true},
		{"duplicate MAXVALUE", "CREATE SEQUENCE seq MAXVALUE 10 MAXVALUE 10", nil, true},
		{"duplicate START WITH", "CREATE SEQUENCE seq START WITH 10 START WITH 10", nil, true},
		{"duplicate CACHE", "CREATE SEQUENCE seq CACHE 10 CACHE 10", nil, true},
		{"duplicate CYCLE", "CREATE SEQUENCE seq CYCLE CYCLE", nil, true},
		{"BAD MINVALUE MAXVALUE", "CREATE SEQUENCE seq MINVALUE 10 MAXVALUE 5", nil, true},
		{"BAD START", "CREATE SEQUENCE seq MINVALUE 5 MAXVALUE 10 START 100", nil, true},
		{"BAD START", "CREATE SEQUENCE seq MINVALUE 5 MAXVALUE 10 START -100", nil, true},
		{"MINVALUE 10 DESC", "CREATE SEQUENCE seq MINVALUE 10 MAXVALUE 100 INCREMENT BY -1", &statement.CreateSequenceStmt{
			Info: database.SequenceInfo{Name: "seq", IncrementBy: -1, Min: 10, Max: 100, Start: 100, Cache: 1},
		}, false},
		{"NO MINVALUE DESC", "CREATE SEQUENCE seq NO MINVALUE MAXVALUE 100 INCREMENT BY -1", &statement.CreateSequenceStmt{
			Info: database.SequenceInfo{Name: "seq", IncrementBy: -1, Min: math.MinInt64, Max: 100, Start: 100, Cache: 1},
		}, false},
		{"NO MAXVALUE DESC", "CREATE SEQUENCE seq NO MINVALUE NO MAXVALUE INCREMENT BY -1", &statement.CreateSequenceStmt{
			Info: database.SequenceInfo{Name: "seq", IncrementBy: -1, Min: math.MinInt64, Max: -1, Start: -1, Cache: 1},
		}, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parser.ParseQuery(test.s)
			if test.errored {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}
