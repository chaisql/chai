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

func TestUnion(t *testing.T) {
	setup := func(t *testing.T, db *genji.DB) {
		t.Helper()

		q := `
CREATE TABLE foo;
CREATE TABLE bar;
CREATE TABLE baz;
INSERT INTO foo (a,b) VALUES (1.0, 1.0), (2.0, 2.0);
INSERT INTO bar (a,b) VALUES (2.0, 2.0), (3.0, 3.0);
INSERT INTO baz (x,y) VALUES ("a", "a"), ("b", "b");
`
		err := db.Exec(q)
		assert.NoError(t, err)
	}

	// --------------------------------------------------------------------------
	t.Run("basic union all", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`SELECT * FROM foo`, func(t *testing.T) {
			q := `
SELECT * FROM foo
UNION ALL
SELECT * FROM bar;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"a": 1.0, "b": 1.0}
{"a": 2.0, "b": 2.0}
{"a": 2.0, "b": 2.0}
{"a": 3.0, "b": 3.0}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("basic union all with diff fields", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`SELECT * FROM foo`, func(t *testing.T) {
			q := `
SELECT * FROM foo
UNION ALL
SELECT * FROM baz;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"a": 1.0, "b": 1.0}
{"a": 2.0, "b": 2.0}
{"x": "a", "y": "a"}
{"x": "b", "y": "b"}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("union all with conditions", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`SELECT * FROM foo WHERE a > 1`, func(t *testing.T) {
			q := `
SELECT * FROM foo WHERE a > 1
UNION ALL
SELECT * FROM baz WHERE x != "b";
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"a": 2.0, "b": 2.0}
{"x": "a", "y": "a"}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("self union all", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`SELECT * FROM foo WHERE a > 1`, func(t *testing.T) {
			q := `
SELECT * FROM foo WHERE a > 1
UNION ALL
SELECT * FROM foo WHERE a <= 1;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"a": 2.0, "b": 2.0}
{"a": 1.0, "b": 1.0}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("multiple unions all", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`SELECT * FROM foo`, func(t *testing.T) {
			q := `
SELECT * FROM foo
UNION ALL
SELECT * FROM bar
UNION ALL
SELECT * FROM baz;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"a": 1.0, "b": 1.0}
{"a": 2.0, "b": 2.0}
{"a": 2.0, "b": 2.0}
{"a": 3.0, "b": 3.0}
{"x": "a", "y": "a"}
{"x": "b", "y": "b"}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("basic union", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`SELECT * FROM foo`, func(t *testing.T) {
			q := `
SELECT * FROM foo
UNION
SELECT * FROM bar;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"a": 1.0, "b": 1.0}
{"a": 2.0, "b": 2.0}
{"a": 3.0, "b": 3.0}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("basic union all with diff fields", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`SELECT * FROM foo`, func(t *testing.T) {
			q := `
SELECT * FROM foo
UNION
SELECT * FROM baz;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"a": 1.0, "b": 1.0}
{"a": 2.0, "b": 2.0}
{"x": "a", "y": "a"}
{"x": "b", "y": "b"}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("union with conditions", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`SELECT * FROM foo WHERE a > 1`, func(t *testing.T) {
			q := `
SELECT * FROM foo WHERE a > 1
UNION
SELECT * FROM baz WHERE x != "b";
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"a": 2.0, "b": 2.0}
{"x": "a", "y": "a"}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("self union", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`SELECT * FROM foo`, func(t *testing.T) {
			q := `
SELECT * FROM foo
UNION
SELECT * FROM foo;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"a": 1.0, "b": 1.0}
{"a": 2.0, "b": 2.0}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("self union with conds", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`SELECT * FROM foo WHERE a > 1`, func(t *testing.T) {
			q := `
SELECT * FROM foo WHERE a > 1
UNION
SELECT * FROM foo WHERE a <= 1;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"a": 1.0, "b": 1.0}
{"a": 2.0, "b": 2.0}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("multiple unions all", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`SELECT * FROM foo`, func(t *testing.T) {
			q := `
SELECT * FROM foo
UNION ALL
SELECT * FROM bar
UNION ALL
SELECT * FROM baz;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"a": 1.0, "b": 1.0}
{"a": 2.0, "b": 2.0}
{"a": 2.0, "b": 2.0}
{"a": 3.0, "b": 3.0}
{"x": "a", "y": "a"}
{"x": "b", "y": "b"}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("combined unions", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`SELECT * FROM foo`, func(t *testing.T) {
			q := `
SELECT * FROM foo
UNION
SELECT * FROM bar
UNION ALL
SELECT * FROM baz;
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"a": 1.0, "b": 1.0}
{"a": 2.0, "b": 2.0}
{"a": 3.0, "b": 3.0}
{"x": "a", "y": "a"}
{"x": "b", "y": "b"}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

}
