package statement_test

import (
	"database/sql"
	"testing"

	_ "github.com/chaisql/chai"
	errs "github.com/chaisql/chai/internal/errors"
	"github.com/stretchr/testify/require"
)

func TestAlterTable(t *testing.T) {
	db, err := sql.Open("chai", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE foo(name TEXT PRIMARY KEY, age INT)")
	require.NoError(t, err)

	// Insert some data into foo
	_, err = db.Exec(`INSERT INTO foo VALUES ('John Doe', 99)`)
	require.NoError(t, err)

	// Renaming the table to the same name should fail.
	_, err = db.Exec("ALTER TABLE foo RENAME TO foo")
	require.ErrorIs(t, err, errs.AlreadyExistsError{Name: "foo"})

	_, err = db.Exec("ALTER TABLE foo RENAME TO bar")
	require.NoError(t, err)

	// Selecting from the old name should fail.
	_, err = db.Exec("SELECT * FROM foo")
	if !errs.IsNotFoundError(err) {
		require.ErrorIs(t, err, errs.NewNotFoundError("foo"))
	}

	var name string
	var age int
	err = db.QueryRow("SELECT * FROM bar").Scan(&name, &age)
	require.NoError(t, err)
	require.Equal(t, "John Doe", name)
	require.Equal(t, 99, age)

	// Renaming a read-only table should fail
	_, err = db.Exec("ALTER TABLE __chai_catalog RENAME TO bar")
	require.Error(t, err)
}
