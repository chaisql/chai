package query_test

import (
	"testing"

	"github.com/asdine/genji"
	"github.com/asdine/genji/database"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/engine/memoryengine"
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
		{"With primary key", "CREATE TABLE test(foo STRING PRIMARY KEY)", false},
		{"With field constraints key", "CREATE TABLE test(foo.a.1.2 STRING, bar.4.0.bat int8)", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.New(memoryengine.NewEngine())
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
		db, err := genji.New(memoryengine.NewEngine())
		require.NoError(t, err)
		defer db.Close()

		err = db.Exec("CREATE TABLE test(foo.bar.1.hello bytes PRIMARY KEY, foo.a.1.2 STRING, bar.4.0.bat int8)")
		require.NoError(t, err)

		err = db.ViewTable("test", func(_ *genji.Tx, tb *database.Table) error {
			cfg, err := tb.Config()
			if err != nil {
				return err
			}

			require.Equal(t, &database.TableConfig{
				PrimaryKey: database.FieldConstraint{
					Path: []string{"foo", "bar", "1", "hello"},
					Type: document.BytesValue,
				},
				FieldConstraints: []database.FieldConstraint{
					{Path: []string{"foo", "a", "1", "2"}, Type: document.TextValue},
					{Path: []string{"bar", "4", "0", "bat"}, Type: document.Int8Value},
				},
			}, cfg)
			return nil
		})
		require.NoError(t, err)
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
			db, err := genji.New(memoryengine.NewEngine())
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
