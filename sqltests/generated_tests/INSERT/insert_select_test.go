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

func TestInsertSelect(t *testing.T) {
	setup := func(t *testing.T, db *genji.DB) {}
	postSetup := func(t *testing.T, db *genji.DB) {}
	setup = func(t *testing.T, db *genji.DB) {
		t.Helper()
		q := `
CREATE TABLE foo;
CREATE TABLE bar;
INSERT INTO bar (a, b) VALUES (1, 10);
`
		err := db.Exec(q)
		assert.NoError(t, err)
	}
	// --------------------------------------------------------------------------
	t.Run("same table", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`INSERT INTO foo SELECT * FROM foo;`, func(t *testing.T) {
			q := `
INSERT INTO foo SELECT * FROM foo;
`
			err := db.Exec(q)
			assert.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})
	})

	// --------------------------------------------------------------------------
	t.Run("No fields / No projection", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`INSERT INTO foo SELECT * FROM bar;`, func(t *testing.T) {
			q := `
INSERT INTO foo SELECT * FROM bar;
SELECT pk(), * FROM foo;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"pk()":1, "a":1.0, "b":10.0}
`
			testutil.RequireStreamEq(t, raw, res, false)
		})
	})

	// --------------------------------------------------------------------------
	t.Run("No fields / Projection", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`INSERT INTO foo SELECT a FROM bar;`, func(t *testing.T) {
			q := `
INSERT INTO foo SELECT a FROM bar;
SELECT pk(), * FROM foo;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"pk()":1, "a":1.0}
`
			testutil.RequireStreamEq(t, raw, res, false)
		})
	})

	// --------------------------------------------------------------------------
	t.Run("With fields / No Projection", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`INSERT INTO foo (a, b) SELECT * FROM bar;`, func(t *testing.T) {
			q := `
INSERT INTO foo (a, b) SELECT * FROM bar;
SELECT pk(), * FROM foo;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"pk()":1, "a":1.0, "b":10.0}
`
			testutil.RequireStreamEq(t, raw, res, false)
		})
	})

	// --------------------------------------------------------------------------
	t.Run("With fields / Projection", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`INSERT INTO foo (c, d) SELECT a, b FROM bar;`, func(t *testing.T) {
			q := `
INSERT INTO foo (c, d) SELECT a, b FROM bar;
SELECT pk(), * FROM foo;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"pk()":1, "c":1.0, "d":10.0}
`
			testutil.RequireStreamEq(t, raw, res, false)
		})
	})

	// --------------------------------------------------------------------------
	t.Run("Too many fields / No Projection", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`INSERT INTO foo (c) SELECT * FROM bar;`, func(t *testing.T) {
			q := `
INSERT INTO foo (c) SELECT * FROM bar;
`
			err := db.Exec(q)
			assert.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})
	})

	// --------------------------------------------------------------------------
	t.Run("Too many fields / Projection", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`INSERT INTO foo (c, d) SELECT a, b, c FROM bar;`, func(t *testing.T) {
			q := `
INSERT INTO foo (c, d) SELECT a, b, c FROM bar;
`
			err := db.Exec(q)
			assert.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})
	})

	// --------------------------------------------------------------------------
	t.Run("Too few fields / No Projection", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`INSERT INTO foo (c, d, e) SELECT * FROM bar;`, func(t *testing.T) {
			q := `
INSERT INTO foo (c, d, e) SELECT * FROM bar;
`
			err := db.Exec(q)
			assert.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})
	})

	// --------------------------------------------------------------------------
	t.Run("Too few fields / Projection", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`INSERT INTO foo (c, d) SELECT a FROM bar`+"`"+`;`, func(t *testing.T) {
			q := `
INSERT INTO foo (c, d) SELECT a FROM bar` + "`" + `;
`
			err := db.Exec(q)
			assert.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})
	})

}
