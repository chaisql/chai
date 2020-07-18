package query_test

import (
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/stretchr/testify/require"
)

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
		{"With field constraints", "CREATE TABLE test(foo.a.1.2 TEXT primary key, bar.4.0.bat INTEGER not null, baz not null)", false},
		{"With no constraints", "CREATE TABLE test(a, b)", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec(test.query)
			if test.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			err = db.ViewTable("test", func(_ *genji.Tx, _ *database.Table) error {
				return nil
			})
			require.NoError(t, err)
		})
	}

	t.Run("constraints", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		t.Run("with fixed size data types", func(t *testing.T) {
			err = db.Exec(`CREATE TABLE test(d double, b bool)`)
			require.NoError(t, err)

			err = db.ViewTable("test", func(_ *genji.Tx, tb *database.Table) error {
				info, err := tb.Info()
				if err != nil {
					return err
				}

				require.Equal(t, []database.FieldConstraint{
					{Path: []string{"d"}, Type: document.DoubleValue},
					{Path: []string{"b"}, Type: document.BoolValue},
				}, info.FieldConstraints)
				return nil
			})
			require.NoError(t, err)

		})

		t.Run("with variable size data types", func(t *testing.T) {
			err = db.Exec(`
				CREATE TABLE test1(
					foo.bar.1.hello bytes PRIMARY KEY, foo.a.1.2 TEXT NOT NULL, bar.4.0.bat integer,
				 	du duration, b blob, t text, a array, d document
				)
			`)
			require.NoError(t, err)

			err = db.ViewTable("test1", func(_ *genji.Tx, tb *database.Table) error {
				info, err := tb.Info()
				if err != nil {
					return err
				}

				require.Equal(t, []database.FieldConstraint{
					{Path: []string{"foo", "bar", "1", "hello"}, Type: document.BlobValue, IsPrimaryKey: true},
					{Path: []string{"foo", "a", "1", "2"}, Type: document.TextValue, IsNotNull: true},
					{Path: []string{"bar", "4", "0", "bat"}, Type: document.IntegerValue},
					{Path: []string{"du"}, Type: document.DurationValue},
					{Path: []string{"b"}, Type: document.BlobValue},
					{Path: []string{"t"}, Type: document.TextValue},
					{Path: []string{"a"}, Type: document.ArrayValue},
					{Path: []string{"d"}, Type: document.DocumentValue},
				}, info.FieldConstraints)
				return nil
			})
			require.NoError(t, err)

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
		{"Unique", "CREATE UNIQUE INDEX IF NOT EXISTS idx ON test (foo.1)", false},
		{"No fields", "CREATE INDEX idx ON test", true},
		{"More than 1 field", "CREATE INDEX idx ON test (foo, bar)", true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
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
