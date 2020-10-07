package query_test

import (
	"context"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/stretchr/testify/require"
)

func TestAlterTable(t *testing.T) {
	ctx := context.Background()

	db, err := genji.Open(ctx, ":memory:")
	require.NoError(t, err)
	defer db.Close()

	err = db.Exec(ctx, "CREATE TABLE foo")
	require.NoError(t, err)

	// Insert some data into foo
	err = db.Exec(ctx, `INSERT INTO foo VALUES {name: "John Doe", age: 99}`)
	require.NoError(t, err)

	// Renaming the table to the same name should fail.
	err = db.Exec(ctx, "ALTER TABLE foo RENAME TO foo")
	require.EqualError(t, err, database.ErrTableAlreadyExists.Error())

	err = db.Exec(ctx, "ALTER TABLE foo RENAME TO bar")
	require.NoError(t, err)

	// Selecting from the old name should fail.
	err = db.Exec(ctx, "SELECT * FROM foo")
	require.EqualError(t, err, database.ErrTableNotFound.Error())

	d, err := db.QueryDocument(ctx, "SELECT * FROM bar")
	data, err := document.MarshalJSON(d)
	require.NoError(t, err)
	require.JSONEq(t, `{"name": "John Doe", "age": 99}`, string(data))

	// Renaming a read-only table should fail
	err = db.Exec(ctx, "ALTER TABLE __genji_tables RENAME TO bar")
	require.Error(t, err)
}
