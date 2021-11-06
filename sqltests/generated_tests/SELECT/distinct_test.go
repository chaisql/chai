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

func TestDistinct(t *testing.T) {
	setup := func(t *testing.T, db *genji.DB) {}
	postSetup := func(t *testing.T, db *genji.DB) {}
	setup = func(t *testing.T, db *genji.DB) {
		t.Helper()
		q := `
CREATE TABLE test;
INSERT INTO test(a, b, c) VALUES
(1, {d: 1}, [true]),
(1, {d: 2}, [false]),
(1, {d: 2}, []),
(2, {d: 3}, []),
(2, {d: 3}, []),
([true], 1, 1.5);
`
		err := db.Exec(q)
		assert.NoError(t, err)
	}
	// --------------------------------------------------------------------------
	t.Run("literal", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`SELECT DISTINCT 'a' FROM test;`, func(t *testing.T) {
			q := `
SELECT DISTINCT 'a' FROM test;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{
    ` + "`" + `"a"` + "`" + `: "a",
}
`
			testutil.RequireStreamEq(t, raw, res, false)
		})
	})

	// --------------------------------------------------------------------------
	t.Run("wildcard", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`SELECT DISTINCT * FROM test;`, func(t *testing.T) {
			q := `
SELECT DISTINCT * FROM test;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{
    a: 1.0,
    b: {d: 1.0},
    c: [true]
}
{
    a: 1.0,
    b: {d: 2.0},
    c: []
}
{
    a: 1.0,
    b: {d: 2.0},
    c: [false]
}
{
    a: 2.0,
    b: {d: 3.0},
    c: []
}
{
    a: [true],
    b: 1.0,
    c: 1.5
}
`
			testutil.RequireStreamEq(t, raw, res, false)
		})
	})

	// --------------------------------------------------------------------------
	t.Run("field path", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`SELECT DISTINCT a FROM test;`, func(t *testing.T) {
			q := `
SELECT DISTINCT a FROM test;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{
    a: 1.0,
}
{
    a: 2.0,
}
{
    a: [true],
}
`
			testutil.RequireStreamEq(t, raw, res, false)
		})
	})

	// --------------------------------------------------------------------------
	t.Run("field path", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`SELECT DISTINCT a FROM test;`, func(t *testing.T) {
			q := `
SELECT DISTINCT a FROM test;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{
    a: 1.0,
}
{
    a: 2.0,
}
{
    a: [true],
}
`
			testutil.RequireStreamEq(t, raw, res, false)
		})
	})

	// --------------------------------------------------------------------------
	t.Run("multiple field paths", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)
		postSetup(t, db)
		t.Run(`SELECT DISTINCT a, b.d FROM test;`, func(t *testing.T) {
			q := `
SELECT DISTINCT a, b.d FROM test;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{
    a: 1.0,
    "b.d": 1.0
}
{
    a: 1.0,
    "b.d": 2.0
}
{
    a: 2.0,
    "b.d": 3.0
}
{
    a: [true],
    "b.d": NULL
}
`
			testutil.RequireStreamEq(t, raw, res, false)
		})
	})

}
