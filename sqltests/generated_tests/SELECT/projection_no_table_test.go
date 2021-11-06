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

func TestProjectionNoTable(t *testing.T) {
	setup := func(t *testing.T, db *genji.DB) {}
	postSetup := func(t *testing.T, db *genji.DB) {}
	setup = func(t *testing.T, db *genji.DB) {
		t.Helper()
		q := `
`
		err := db.Exec(q)
		assert.NoError(t, err)
	}
	// --------------------------------------------------------------------------
	t.Run("simple projection", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`SELECT 1;`, func(t *testing.T) {
			q := `
SELECT 1;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"1": 1}
`
			testutil.RequireStreamEq(t, raw, res, false)
		})
	})

	// --------------------------------------------------------------------------
	t.Run("complex expression", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`SELECT 1 + 1 * 2 / 4;`, func(t *testing.T) {
			q := `
SELECT 1 + 1 * 2 / 4;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"1 + 1 * 2 / 4": 1}
`
			testutil.RequireStreamEq(t, raw, res, false)
		})
	})

	// --------------------------------------------------------------------------
	t.Run("with spaces", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`SELECT     1  + 1 *      2 /                    4;`, func(t *testing.T) {
			q := `
SELECT     1  + 1 *      2 /                    4;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"1 + 1 * 2 / 4": 1}
`
			testutil.RequireStreamEq(t, raw, res, false)
		})
	})

	// --------------------------------------------------------------------------
	t.Run("escaping, double quotes", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`SELECT '"A"';`, func(t *testing.T) {
			q := `
SELECT '"A"';
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{` + "`" + `"\\"A\\""` + "`" + `: "\"A\""}
`
			testutil.RequireStreamEq(t, raw, res, false)
		})
	})

	// --------------------------------------------------------------------------
	t.Run("escaping, single quotes", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`SELECT "'A'";`, func(t *testing.T) {
			q := `
SELECT "'A'";
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{` + "`" + `"'A'"` + "`" + `: "'A'"}
`
			testutil.RequireStreamEq(t, raw, res, false)
		})
	})

	// --------------------------------------------------------------------------
	t.Run("document", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`SELECT {a: 1, b: 2 + 1};`, func(t *testing.T) {
			q := `
SELECT {a: 1, b: 2 + 1};
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"{a: 1, b: 2 + 1}":{"a":1,"b":3}}
`
			testutil.RequireStreamEq(t, raw, res, false)
		})
	})

	// --------------------------------------------------------------------------
	t.Run("aliases", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`SELECT 1 AS A;`, func(t *testing.T) {
			q := `
SELECT 1 AS A;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"A": 1}
`
			testutil.RequireStreamEq(t, raw, res, false)
		})
	})

	// --------------------------------------------------------------------------
	t.Run("aliases with cast", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`SELECT CAST(1 AS DOUBLE) AS A;`, func(t *testing.T) {
			q := `
SELECT CAST(1 AS DOUBLE) AS A;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"A": 1.0}
`
			testutil.RequireStreamEq(t, raw, res, false)
		})
	})

	// --------------------------------------------------------------------------
	t.Run("pk()", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`SELECT pk();`, func(t *testing.T) {
			q := `
SELECT pk();
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"pk()": null}
`
			testutil.RequireStreamEq(t, raw, res, false)
		})
	})

	// --------------------------------------------------------------------------
	t.Run("field", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`SELECT a;`, func(t *testing.T) {
			q := `
SELECT a;
`
			err := db.Exec(q)
			assert.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})
	})

	// --------------------------------------------------------------------------
	t.Run("wildcard", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`SELECT *;`, func(t *testing.T) {
			q := `
SELECT *;
`
			err := db.Exec(q)
			assert.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})
	})

}
