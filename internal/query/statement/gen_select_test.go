/*
* CODE GENERATED AUTOMATICALLY WITH github.com/genjidb/genji/dev/gensqltest
* THIS FILE SHOULD NOT BE EDITED BY HAND
 */
package statement_test

import (
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
)

func TestGenSelect(t *testing.T) {
	setup := func(t *testing.T, db *genji.DB) {
		t.Helper()

		q := `
CREATE TABLE foo;
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
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("complex expression", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

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
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("with spaces", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

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
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("escaping, double quotes", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`SELECT '"A"';`, func(t *testing.T) {
			q := `
SELECT '"A"';
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"'\"A\"'": "\"A\""}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("escaping, single quotes", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`SELECT "'A'";`, func(t *testing.T) {
			q := `
SELECT "'A'";
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"'\\'A\\''": "'A'"}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("aliases", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

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
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("aliases with cast", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

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
			testutil.RequireStreamEq(t, raw, res)
		})

	})

}
