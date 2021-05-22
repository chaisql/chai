package query_test

import (
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/testutil"
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
	res, err := db.Query("SELECT table_name FROM __genji_tables")
	require.NoError(t, err)
	var tables []string
	err = res.Iterate(func(d document.Document) error {
		v, err := d.GetByField("table_name")
		if err != nil {
			return err
		}
		tables = append(tables, v.V.(string))
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, res.Close())

	require.Len(t, tables, 2)

	// Dropping a read-only table should fail.
	err = db.Exec("DROP TABLE __genji_tables")
	require.Error(t, err)
}

func TestDropIndex(t *testing.T) {
	_, tx, cleanup := testutil.NewTestTx(t)
	defer cleanup()

	testutil.MustExec(t, tx, `
		CREATE TABLE test1(foo text); CREATE INDEX idx_test1_foo ON test1(foo);
		CREATE TABLE test2(bar text); CREATE INDEX idx_test2_bar ON test2(bar);
	`)

	testutil.MustExec(t, tx, "DROP INDEX idx_test2_bar")

	// Assert that the good index has been dropped.
	var indexes []string
	indexes = tx.Catalog.ListIndexes("")
	require.Len(t, indexes, 1)
	require.Equal(t, "idx_test1_foo", indexes[0])
}
