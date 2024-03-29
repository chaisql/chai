package statement_test

import (
	"testing"

	"github.com/chaisql/chai"
	errs "github.com/chaisql/chai/internal/errors"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestDropTable(t *testing.T) {
	db, err := chai.Open(":memory:")
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Connect()
	require.NoError(t, err)
	defer conn.Close()

	err = conn.Exec("CREATE TABLE test1(a INT UNIQUE); CREATE TABLE test2(a INT); CREATE TABLE test3(a INT)")
	require.NoError(t, err)

	err = conn.Exec("DROP TABLE test1")
	require.NoError(t, err)

	err = conn.Exec("DROP TABLE IF EXISTS test1")
	require.NoError(t, err)

	// Dropping a table that doesn't exist without "IF EXISTS"
	// should return an error.
	err = conn.Exec("DROP TABLE test1")
	require.Error(t, err)

	// Assert that no other table has been dropped.
	res, err := conn.Query("SELECT name FROM __chai_catalog WHERE type = 'table'")
	require.NoError(t, err)
	var tables []string
	err = res.Iterate(func(r *chai.Row) error {
		var name string
		err := r.ScanColumn("name", &name)
		if err != nil {
			return err
		}
		tables = append(tables, name)
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, res.Close())

	require.Equal(t, []string{"__chai_catalog", "__chai_sequence", "test2", "test3"}, tables)

	// Assert the unique index test1_a_idx, created upon the creation of the table,
	// has been dropped as well.
	_, err = conn.QueryRow("SELECT 1 FROM __chai_catalog WHERE name = 'test1_a_idx'")
	require.Error(t, err)

	// Assert the rowid sequence test1_seq, created upon the creation of the table,
	// has been dropped as well.
	_, err = conn.QueryRow("SELECT 1 FROM __chai_catalog WHERE name = 'test1_seq'")
	require.Error(t, err)
	_, err = conn.QueryRow("SELECT 1 FROM __chai_sequence WHERE name = 'test1_seq'")
	require.Error(t, err)

	// Dropping a read-only table should fail.
	err = conn.Exec("DROP TABLE __chai_catalog")
	require.Error(t, err)
}

func TestDropIndex(t *testing.T) {
	db, tx, cleanup := testutil.NewTestTx(t)
	defer cleanup()

	testutil.MustExec(t, db, tx, `
		CREATE TABLE test1(foo text, bar int unique); CREATE INDEX idx_test1_foo ON test1(foo);
		CREATE TABLE test2(bar text); CREATE INDEX idx_test2_bar ON test2(bar);
	`)

	testutil.MustExec(t, db, tx, "DROP INDEX idx_test2_bar")

	// Assert that the good index has been dropped.
	indexes := tx.Catalog.ListIndexes("")
	require.Len(t, indexes, 2)
	require.Equal(t, "idx_test1_foo", indexes[0])
	require.Equal(t, "test1_bar_idx", indexes[1])

	// Dropping a non existing index with IF EXISTS should not fail.
	err := testutil.Exec(db, tx, "DROP INDEX IF EXISTS unknown")
	require.NoError(t, err)

	// Dropping an index created with a table constraint should fail.
	err = testutil.Exec(db, tx, "DROP INDEX test1_bar_idx")
	require.Error(t, err)
}

func TestDropSequence(t *testing.T) {
	db, tx, cleanup := testutil.NewTestTx(t)
	defer cleanup()

	testutil.MustExec(t, db, tx, `
		CREATE TABLE test1(foo int);
		CREATE SEQUENCE seq1;
		CREATE SEQUENCE seq2;
	`)

	testutil.MustExec(t, db, tx, "DROP SEQUENCE seq1")

	// Assert that the good index has been dropped.
	_, err := tx.Catalog.GetSequence("seq1")
	require.IsType(t, &errs.NotFoundError{}, errors.Unwrap(err))
	_, err = tx.Catalog.GetSequence("seq2")
	require.NoError(t, err)

	// Dropping a non existing sequence with IF EXISTS should not fail.
	err = testutil.Exec(db, tx, "DROP SEQUENCE IF EXISTS unknown")
	require.NoError(t, err)

	// Dropping a sequence created with a table constraint should fail.
	err = testutil.Exec(db, tx, "DROP SEQUENCE test1_seq")
	require.Error(t, err)
}
