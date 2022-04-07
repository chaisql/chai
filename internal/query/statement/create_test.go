package statement_test

import (
	"bytes"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
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

func TestCreateTable(t *testing.T) {
	tests := []struct {
		name  string
		query string
		fails bool
	}{
		{"Basic", `CREATE TABLE test`, false},
		{"Exists", "CREATE TABLE test;CREATE TABLE test", true},
		{"If not exists", "CREATE TABLE IF NOT EXISTS test", false},
		{"If not exists, twice", "CREATE TABLE IF NOT EXISTS test;CREATE TABLE IF NOT EXISTS test", false},
		{"With primary key", "CREATE TABLE test(foo TEXT PRIMARY KEY)", false},
		{"With field constraints", "CREATE TABLE test(foo.a[1][2] TEXT primary key, bar[4][0].bat INTEGER not null, baz not null)", false},
		{"With no constraints", "CREATE TABLE test(a, b)", true},
		{"With coherent constraint(common)", "CREATE TABLE test(a DOCUMENT, a.b ARRAY, a.b[0] TEXT);", false},
		{"With coherent constraint(document)", "CREATE TABLE test(a DOCUMENT, a.b TEXT);", false},
		{"With coherent constraint(array)", "CREATE TABLE test(a ARRAY, a[0] TEXT);", false},
		{"With incoherent constraint(any)", "CREATE TABLE test(a, a.b[0] TEXT);", true},
		{"With incoherent constraint(common)", "CREATE TABLE test(a INTEGER, a.b[0] TEXT);", true},
		{"With incoherent constraint(common)", "CREATE TABLE test(a DOCUMENT, a.b[0] TEXT, a.b.c TEXT);", true},
		{"With incoherent constraint(common)", "CREATE TABLE test(a DOCUMENT, a.b.c TEXT, a.b[0] TEXT);", true},
		{"With incoherent constraint(document)", "CREATE TABLE test(a INTEGER, a.b TEXT);", true},
		{"With incoherent constraint(array)", "CREATE TABLE test(a INTEGER, a[0] TEXT);", true},
		{"With duplicate constraints", "CREATE TABLE test(a INTEGER, a TEXT);", true},
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

			_, err = db.Catalog.GetTable(tx, "test")
			assert.NoError(t, err)
		})
	}

	t.Run("constraints", func(t *testing.T) {
		t.Run("with fixed size data types", func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, `CREATE TABLE test(d double, b bool)`)

			tb, err := db.Catalog.GetTable(tx, "test")
			assert.NoError(t, err)

			require.Equal(t, database.FieldConstraints{
				{Path: testutil.ParseDocumentPath(t, "d"), Type: types.DoubleValue},
				{Path: testutil.ParseDocumentPath(t, "b"), Type: types.BoolValue},
			}, tb.Info.FieldConstraints)
			assert.NoError(t, err)
		})

		t.Run("with variable size data types", func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, `
				CREATE TABLE test1(
					foo.bar[1].hello bytes PRIMARY KEY, foo.a[1][2] TEXT NOT NULL, bar[4][0].bat integer, b blob, t text, a array, d document
				)
			`)

			tb, err := db.Catalog.GetTable(tx, "test1")
			assert.NoError(t, err)

			require.Equal(t, database.FieldConstraints{
				{Path: testutil.ParseDocumentPath(t, "foo"), Type: types.DocumentValue, IsInferred: true,
					InferredBy: []document.Path{
						testutil.ParseDocumentPath(t, "foo.bar[1].hello"),
						testutil.ParseDocumentPath(t, "foo.a[1][2]"),
					}},
				{Path: testutil.ParseDocumentPath(t, "foo.bar"), Type: types.ArrayValue, IsInferred: true,
					InferredBy: []document.Path{
						testutil.ParseDocumentPath(t, "foo.bar[1].hello"),
					}},
				{Path: testutil.ParseDocumentPath(t, "foo.bar[1]"), Type: types.DocumentValue, IsInferred: true,
					InferredBy: []document.Path{
						testutil.ParseDocumentPath(t, "foo.bar[1].hello"),
					}},
				{Path: testutil.ParseDocumentPath(t, "foo.bar[1].hello"), Type: types.BlobValue},
				{Path: testutil.ParseDocumentPath(t, "foo.a"), Type: types.ArrayValue, IsInferred: true,
					InferredBy: []document.Path{
						testutil.ParseDocumentPath(t, "foo.a[1][2]"),
					}},
				{Path: testutil.ParseDocumentPath(t, "foo.a[1]"), Type: types.ArrayValue, IsInferred: true,
					InferredBy: []document.Path{
						testutil.ParseDocumentPath(t, "foo.a[1][2]"),
					}},
				{Path: testutil.ParseDocumentPath(t, "foo.a[1][2]"), Type: types.TextValue, IsNotNull: true},
				{Path: testutil.ParseDocumentPath(t, "bar"), Type: types.ArrayValue, IsInferred: true,
					InferredBy: []document.Path{
						testutil.ParseDocumentPath(t, "bar[4][0].bat"),
					}},
				{Path: testutil.ParseDocumentPath(t, "bar[4]"), Type: types.ArrayValue, IsInferred: true,
					InferredBy: []document.Path{
						testutil.ParseDocumentPath(t, "bar[4][0].bat"),
					}},
				{Path: testutil.ParseDocumentPath(t, "bar[4][0]"), Type: types.DocumentValue, IsInferred: true,
					InferredBy: []document.Path{
						testutil.ParseDocumentPath(t, "bar[4][0].bat"),
					}},
				{Path: testutil.ParseDocumentPath(t, "bar[4][0].bat"), Type: types.IntegerValue},
				{Path: testutil.ParseDocumentPath(t, "b"), Type: types.BlobValue},
				{Path: testutil.ParseDocumentPath(t, "t"), Type: types.TextValue},
				{Path: testutil.ParseDocumentPath(t, "a"), Type: types.ArrayValue},
				{Path: testutil.ParseDocumentPath(t, "d"), Type: types.DocumentValue},
			}, tb.Info.FieldConstraints)
			assert.NoError(t, err)
		})

		t.Run("with variable aliases data types", func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, `
				CREATE TABLE test2(
					foo.bar[1].hello bytes PRIMARY KEY, foo.a[1][2] VARCHAR(255) NOT NULL, bar[4][0].bat tinyint,
				 	dp double precision, r real, b bigint, m mediumint, eight int8, ii int2, c character(64)
				)
			`)

			tb, err := db.Catalog.GetTable(tx, "test2")
			assert.NoError(t, err)

			require.Equal(t, database.FieldConstraints{{Path: testutil.ParseDocumentPath(t, "foo"), Type: types.DocumentValue, IsInferred: true,
				InferredBy: []document.Path{
					testutil.ParseDocumentPath(t, "foo.bar[1].hello"),
					testutil.ParseDocumentPath(t, "foo.a[1][2]"),
				}},
				{Path: testutil.ParseDocumentPath(t, "foo.bar"), Type: types.ArrayValue, IsInferred: true,
					InferredBy: []document.Path{
						testutil.ParseDocumentPath(t, "foo.bar[1].hello"),
					}},
				{Path: testutil.ParseDocumentPath(t, "foo.bar[1]"), Type: types.DocumentValue, IsInferred: true,
					InferredBy: []document.Path{
						testutil.ParseDocumentPath(t, "foo.bar[1].hello"),
					}},
				{Path: testutil.ParseDocumentPath(t, "foo.bar[1].hello"), Type: types.BlobValue},
				{Path: testutil.ParseDocumentPath(t, "foo.a"), Type: types.ArrayValue, IsInferred: true,
					InferredBy: []document.Path{
						testutil.ParseDocumentPath(t, "foo.a[1][2]"),
					}},
				{Path: testutil.ParseDocumentPath(t, "foo.a[1]"), Type: types.ArrayValue, IsInferred: true,
					InferredBy: []document.Path{
						testutil.ParseDocumentPath(t, "foo.a[1][2]"),
					}},
				{Path: testutil.ParseDocumentPath(t, "foo.a[1][2]"), Type: types.TextValue, IsNotNull: true},
				{Path: testutil.ParseDocumentPath(t, "bar"), Type: types.ArrayValue, IsInferred: true,
					InferredBy: []document.Path{
						testutil.ParseDocumentPath(t, "bar[4][0].bat"),
					}},
				{Path: testutil.ParseDocumentPath(t, "bar[4]"), Type: types.ArrayValue, IsInferred: true,
					InferredBy: []document.Path{
						testutil.ParseDocumentPath(t, "bar[4][0].bat"),
					}},
				{Path: testutil.ParseDocumentPath(t, "bar[4][0]"), Type: types.DocumentValue, IsInferred: true,
					InferredBy: []document.Path{
						testutil.ParseDocumentPath(t, "bar[4][0].bat"),
					}},
				{Path: testutil.ParseDocumentPath(t, "bar[4][0].bat"), Type: types.IntegerValue},
				{Path: testutil.ParseDocumentPath(t, "dp"), Type: types.DoubleValue},
				{Path: testutil.ParseDocumentPath(t, "r"), Type: types.DoubleValue},
				{Path: testutil.ParseDocumentPath(t, "b"), Type: types.IntegerValue},
				{Path: testutil.ParseDocumentPath(t, "m"), Type: types.IntegerValue},
				{Path: testutil.ParseDocumentPath(t, "eight"), Type: types.IntegerValue},
				{Path: testutil.ParseDocumentPath(t, "ii"), Type: types.IntegerValue},
				{Path: testutil.ParseDocumentPath(t, "c"), Type: types.TextValue},
			}, tb.Info.FieldConstraints)
			assert.NoError(t, err)
		})

		t.Run("default values", func(t *testing.T) {
			tests := []struct {
				name        string
				query       string
				constraints database.FieldConstraints
				fails       bool
			}{
				{"With default, no type and integer default", "CREATE TABLE test(foo DEFAULT 10)", database.FieldConstraints{{Path: testutil.ParseDocumentPath(t, "foo"), DefaultValue: expr.Constraint(testutil.IntegerValue(10))}}, false},
				{"With default, double type and integer default", "CREATE TABLE test(foo DOUBLE DEFAULT 10)", database.FieldConstraints{{Path: testutil.ParseDocumentPath(t, "foo"), Type: types.DoubleValue, DefaultValue: expr.Constraint(testutil.IntegerValue(10))}}, false},
				{"With default, some type and compatible default", "CREATE TABLE test(foo BOOL DEFAULT 10)", database.FieldConstraints{{Path: testutil.ParseDocumentPath(t, "foo"), Type: types.BoolValue, DefaultValue: expr.Constraint(testutil.IntegerValue(10))}}, false},
				{"With default, some type and incompatible default", "CREATE TABLE test(foo BOOL DEFAULT 10.5)", nil, true},
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

					tb, err := db.Catalog.GetTable(tx, "test")
					assert.NoError(t, err)

					for _, fc := range test.constraints {
						if fc.DefaultValue != nil {
							fc.DefaultValue.(*expr.ConstraintExpr).Catalog = db.Catalog
						}
					}
					require.Equal(t, test.constraints, tb.Info.FieldConstraints)
				})
			}
		})

		t.Run("unique", func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, "CREATE TABLE test (a INT UNIQUE, b DOUBLE UNIQUE, c UNIQUE)")

			tb, err := db.Catalog.GetTable(tx, "test")
			assert.NoError(t, err)
			require.Len(t, tb.Info.FieldConstraints, 2)
			require.Len(t, tb.Info.TableConstraints, 3)

			require.Equal(t, &database.FieldConstraint{
				Path: testutil.ParseDocumentPath(t, "a"),
				Type: types.IntegerValue,
			}, tb.Info.FieldConstraints[0])

			require.Equal(t, &database.FieldConstraint{
				Path: testutil.ParseDocumentPath(t, "b"),
				Type: types.DoubleValue,
			}, tb.Info.FieldConstraints[1])

			require.Equal(t, &database.TableConstraint{
				Name:   "test_a_unique",
				Paths:  testutil.ParseDocumentPaths(t, "a"),
				Unique: true,
			}, tb.Info.TableConstraints[0])

			require.Equal(t, &database.TableConstraint{
				Name:   "test_b_unique",
				Paths:  testutil.ParseDocumentPaths(t, "b"),
				Unique: true,
			}, tb.Info.TableConstraints[1])

			require.Equal(t, &database.TableConstraint{
				Name:   "test_c_unique",
				Paths:  testutil.ParseDocumentPaths(t, "c"),
				Unique: true,
			}, tb.Info.TableConstraints[2])

			info, err := db.Catalog.GetIndexInfo("test_a_idx")
			assert.NoError(t, err)
			require.True(t, info.Unique)

			info, err = db.Catalog.GetIndexInfo("test_b_idx")
			assert.NoError(t, err)
			require.True(t, info.Unique)

			info, err = db.Catalog.GetIndexInfo("test_c_idx")
			assert.NoError(t, err)
			require.True(t, info.Unique)
			assert.NoError(t, err)
		})

		t.Run("default with nested doc", func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			err := testutil.Exec(db, tx, "CREATE TABLE test (a.b.c TEXT DEFAULT 1 + 1)")
			assert.NoError(t, err)

			var buf bytes.Buffer
			err = testutil.IteratorToJSONArray(&buf, testutil.MustQuery(t, db, tx, "INSERT INTO test VALUES {}; SELECT * FROM test"))
			assert.NoError(t, err)
			require.JSONEq(t, `[{"a": {"b": {"c": "2"}}}]`, buf.String())
		})
	})
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
		{"Unique", "CREATE UNIQUE INDEX IF NOT EXISTS idx ON test (foo[1])", false},
		{"No name", "CREATE UNIQUE INDEX ON test (foo[1])", false},
		{"No name if not exists", "CREATE UNIQUE INDEX IF NOT EXISTS ON test (foo[1])", true},
		{"No fields", "CREATE INDEX idx ON test", true},
		{"Composite (2)", "CREATE INDEX idx ON test (foo, bar)", false},
		{"Composite (4)", "CREATE INDEX idx ON test (foo, bar, baz, baf)", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, "CREATE TABLE test")

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
