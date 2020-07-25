package query_test

import (
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/database"
	"github.com/stretchr/testify/require"
)

func TestDropTable(t *testing.T) {
	db, err := genji.Open(":memory:")
	require.NoError(t, err)
	defer db.Close()

	err = db.Exec("CREATE TABLE test1; CREATE TABLE test2; CREATE TABLE test3")
	require.NoError(t, err)

	err = db.Exec("DROP TABLE test1")
	require.NoError(t, err)

	err = db.Exec("DROP TABLE IF EXISTS test1")
	require.NoError(t, err)

	// Dropping a table that doesn't exist without "IF EXISTS"
	// should return an error.
	err = db.Exec("DROP TABLE test1")
	require.Error(t, err)

	// Assert that only the table `test1` has been dropped.
	var tables []string
	err = db.View(func(tx *genji.Tx) error {
		tables = tx.ListTables()
		return nil
	})
	require.Len(t, tables, 2)
}

func TestDropIndex(t *testing.T) {
	db, err := genji.Open(":memory:")
	require.NoError(t, err)
	defer db.Close()

	err = db.Exec(`
		CREATE TABLE test1(foo text); CREATE INDEX idx_test1_foo ON test1(foo);
		CREATE TABLE test2(bar text); CREATE INDEX idx_test2_bar ON test2(bar);
	`)
	require.NoError(t, err)

	err = db.Exec("DROP INDEX idx_test2_bar")
	require.NoError(t, err)

	// Assert that the good index has been dropped.
	var indexes []*database.IndexConfig
	err = db.View(func(tx *genji.Tx) error {
		var err error
		indexes, err = tx.ListIndexes()
		return err
	})
	require.Len(t, indexes, 1)
	require.Equal(t, "test1", indexes[0].TableName)
	require.Equal(t, "idx_test1_foo", indexes[0].IndexName)
	require.Equal(t, false, indexes[0].Unique)
}
