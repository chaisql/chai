package statement_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
)

func ParseDocumentPath(t testing.TB, str string) document.Path {
	vp, err := parser.ParsePath(str)
	assert.NoError(t, err)
	return vp
}

func ParseDocumentPaths(t testing.TB, str ...string) []document.Path {
	var paths []document.Path
	for _, s := range str {
		paths = append(paths, ParseDocumentPath(t, s))
	}

	return paths
}

func TestCreateIndex(t *testing.T) {
	tests := []struct {
		name  string
		query string
		fails bool
	}{
		{"Basic", "CREATE INDEX idx ON test (foo)", false},
		{"If not exists", "CREATE INDEX IF NOT EXISTS idx ON test (foo.bar)", false},
		{"Duplicate", "CREATE INDEX idx ON test (foo.bar);CREATE INDEX idx ON test (foo.bar)", true},
		{"Unique", "CREATE UNIQUE INDEX IF NOT EXISTS idx ON test (foo)", false},
		{"No name", "CREATE UNIQUE INDEX ON test (foo)", false},
		{"No name if not exists", "CREATE UNIQUE INDEX IF NOT EXISTS ON test (foo)", true},
		{"No fields", "CREATE INDEX idx ON test", true},
		{"Composite (2)", "CREATE INDEX idx ON test (foo, baz)", false},
		{"Composite (3)", "CREATE INDEX idx ON test (foo, baz, baf)", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, "CREATE TABLE test(foo (bar TEXT), baz any, baf any)")

			err := testutil.Exec(db, tx, test.query)
			if test.fails {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestCreateSequence(t *testing.T) {
	tests := []struct {
		name  string
		query string
		fails bool
	}{
		{"Basic", "CREATE SEQUENCE seq", false},
		{"If not exists", "CREATE SEQUENCE IF NOT EXISTS seq", false},
		{"AS integer", "CREATE SEQUENCE seq AS TINYINT", false},
		{"AS double", "CREATE SEQUENCE seq AS DOUBLE", true},
		{"INCREMENT", "CREATE SEQUENCE seq INCREMENT 10", false},
		{"INCREMENT BY", "CREATE SEQUENCE seq INCREMENT BY 10", false},
		{"INCREMENT BY 0", "CREATE SEQUENCE seq INCREMENT BY 0", true},
		{"NO MINVALUE", "CREATE SEQUENCE seq NO MINVALUE", false},
		{"NO MAXVALUE", "CREATE SEQUENCE seq NO MAXVALUE", false},
		{"NO CYCLE", "CREATE SEQUENCE seq NO CYCLE", false},
		{"NO SUGAR", "CREATE SEQUENCE seq NO SUGAR", true},
		{"MINVALUE 10", "CREATE SEQUENCE seq MINVALUE 10", false},
		{"MINVALUE 'hello'", "CREATE SEQUENCE seq MINVALUE 'hello'", true},
		{"MAXVALUE 10", "CREATE SEQUENCE seq MAXVALUE 10", false},
		{"MAXVALUE 'hello'", "CREATE SEQUENCE seq MAXVALUE 'hello'", true},
		{"START WITH 10", "CREATE SEQUENCE seq START WITH 10", false},
		{"START WITH 'hello'", "CREATE SEQUENCE seq START WITH 'hello'", true},
		{"START 10", "CREATE SEQUENCE seq START 10", false},
		{"CACHE 10", "CREATE SEQUENCE seq CACHE 10", false},
		{"CACHE 'hello'", "CREATE SEQUENCE seq CACHE 'hello'", true},
		{"CACHE -10", "CREATE SEQUENCE seq CACHE -10", true},
		{"CYCLE", "CREATE SEQUENCE seq CYCLE", false},
		{"Order 1", `
			CREATE SEQUENCE IF NOT EXISTS seq
			AS INTEGER
			INCREMENT BY 2
			NO MINVALUE
			MAXVALUE 10
			START WITH 5
			CACHE 5
			CYCLE
		`, false},
		{"Order 2", `
			CREATE SEQUENCE IF NOT EXISTS seq
			CYCLE
			MAXVALUE 10
			INCREMENT BY 2
			START WITH 5
			AS INTEGER
			NO MINVALUE
			CACHE 5
		`, false},
		{"NO MINVALUE with MINVALUE 10", "CREATE SEQUENCE seq NO MINVALUE MINVALUE 10", true},
		{"NO MAXVALUE with MAXVALUE 10", "CREATE SEQUENCE seq NO MAXVALUE MAXVALUE 10", true},
		{"NO CYCLE with CYCLE", "CREATE SEQUENCE seq NO MAXVALUE MAXVALUE 10", true},
		{"duplicate AS INT", "CREATE SEQUENCE seq AS INT AS INT", true},
		{"duplicate INCREMENT BY", "CREATE SEQUENCE seq INCREMENT BY 10 INCREMENT BY 10", true},
		{"duplicate NO MINVALUE", "CREATE SEQUENCE seq NO MINVALUE NO MINVALUE", true},
		{"duplicate NO MAXVALUE", "CREATE SEQUENCE seq NO MAXVALUE NO MAXVALUE", true},
		{"duplicate NO CYCLE", "CREATE SEQUENCE seq NO CYCLE NO CYCLE", true},
		{"duplicate MINVALUE", "CREATE SEQUENCE seq MINVALUE 10 MINVALUE 10", true},
		{"duplicate MAXVALUE", "CREATE SEQUENCE seq MAXVALUE 10 MAXVALUE 10", true},
		{"duplicate START WITH", "CREATE SEQUENCE seq START WITH 10 START WITH 10", true},
		{"duplicate CACHE", "CREATE SEQUENCE seq CACHE 10 CACHE 10", true},
		{"duplicate CYCLE", "CREATE SEQUENCE seq CYCLE CYCLE", true},
		{"BAD MINVALUE MAXVALUE", "CREATE SEQUENCE seq MINVALUE 10 MAXVALUE 5", true},
		{"BAD START", "CREATE SEQUENCE seq MINVALUE 5 MAXVALUE 10 START 100", true},
		{"BAD START", "CREATE SEQUENCE seq MINVALUE 5 MAXVALUE 10 START -100", true},
		{"MINVALUE 10 DESC", "CREATE SEQUENCE seq MINVALUE 10 MAXVALUE 100 INCREMENT BY -1", false},
		{"NO MINVALUE DESC", "CREATE SEQUENCE seq NO MINVALUE MAXVALUE 100 INCREMENT BY -1", false},
		{"NO MAXVALUE DESC", "CREATE SEQUENCE seq NO MINVALUE NO MAXVALUE INCREMENT BY -1", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			err := testutil.Exec(db, tx, test.query)
			if test.fails {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}
