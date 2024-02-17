package statement_test

import (
	"testing"

	"github.com/chaisql/chai"
	errs "github.com/chaisql/chai/internal/errors"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/stretchr/testify/require"
)

func TestAlterTable(t *testing.T) {
	db, err := chai.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	err = db.Exec("CREATE TABLE foo(name TEXT, age INT)")
	assert.NoError(t, err)

	// Insert some data into foo
	err = db.Exec(`INSERT INTO foo VALUES ('John Doe', 99)`)
	assert.NoError(t, err)

	// Renaming the table to the same name should fail.
	err = db.Exec("ALTER TABLE foo RENAME TO foo")
	assert.ErrorIs(t, err, errs.AlreadyExistsError{Name: "foo"})

	err = db.Exec("ALTER TABLE foo RENAME TO bar")
	assert.NoError(t, err)

	// Selecting from the old name should fail.
	err = db.Exec("SELECT * FROM foo")
	if !errs.IsNotFoundError(err) {
		assert.ErrorIs(t, err, errs.NewNotFoundError("foo"))
	}

	r, err := db.QueryRow("SELECT * FROM bar")
	assert.NoError(t, err)
	data, err := r.MarshalJSON()
	assert.NoError(t, err)
	require.JSONEq(t, `{"name": "John Doe", "age": 99}`, string(data))

	// Renaming a read-only table should fail
	err = db.Exec("ALTER TABLE __chai_catalog RENAME TO bar")
	assert.Error(t, err)
}
