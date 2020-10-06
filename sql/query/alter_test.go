package query_test

import (
	"errors"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/stretchr/testify/require"
)

func TestAlterTable(t *testing.T) {
	db, err := genji.Open(":memory:")
	require.NoError(t, err)
	defer db.Close()

	err = db.Exec("CREATE TABLE foo")
	require.NoError(t, err)

	// Insert some data into foo
	err = db.Exec(`INSERT INTO foo VALUES {name: "John Doe", age: 99}`)
	require.NoError(t, err)

	// Renaming the table to the same name should fail.
	err = db.Exec("ALTER TABLE foo RENAME TO foo")
	require.EqualError(t, err, database.ErrTableAlreadyExists.Error())

	err = db.Exec("ALTER TABLE foo RENAME TO bar")
	require.NoError(t, err)

	// Selecting from the old name should fail.
	err = db.Exec("SELECT * FROM foo")
	if !errors.Is(err, database.ErrTableNotFound) {
		require.Equal(t, err, database.ErrTableNotFound)
	}

	d, err := db.QueryDocument("SELECT * FROM bar")
	data, err := document.MarshalJSON(d)
	require.NoError(t, err)
	require.JSONEq(t, `{"name": "John Doe", "age": 99}`, string(data))

	// Renaming a read-only table should fail
	err = db.Exec("ALTER TABLE __genji_tables RENAME TO bar")
	require.Error(t, err)
}
