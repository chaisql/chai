/*
* CODE GENERATED AUTOMATICALLY WITH github.com/genjidb/genji/dev/gensqltest
* THIS FILE SHOULD NOT BE EDITED BY HAND
 */
package generated_test

import (
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
)

func TestProjectionTable(t *testing.T) {
	setup := func(t *testing.T, db *genji.DB) {
		t.Helper()

		q := `
CREATE TABLE foo;
INSERT INTO foo(a, b, c) VALUES (1, {a: 1}, [true]);
`
		err := db.Exec(q)
		assert.NoError(t, err)
	}

	// --------------------------------------------------------------------------
	t.Run("wildcard", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`SELECT * FROM foo;`, func(t *testing.T) {
			q := `
SELECT * FROM foo;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"a": 1.0, "b": {"a": 1.0}, "c": [true]}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("multiple wildcards", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`SELECT *, * FROM foo;`, func(t *testing.T) {
			q := `
SELECT *, * FROM foo;
`
			err := db.Exec(q)
			assert.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("field paths", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`SELECT a, b, c FROM foo;`, func(t *testing.T) {
			q := `
SELECT a, b, c FROM foo;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{
    "a": 1.0,
    "b": {"a": 1.0},
    "c": [true]
}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("field path, wildcards and expressions", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`SELECT a AS A, b.a + 1, * FROM foo;`, func(t *testing.T) {
			q := `
SELECT a AS A, b.a + 1, * FROM foo;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{
    "A": 1.0,
    "b.a + 1": 2.0,
    "a": 1.0,
    "b": {"a": 1.0},
    "c": [true]
}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

}
