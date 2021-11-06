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
	setup := func(t *testing.T, db *genji.DB) {}
	postSetup := func(t *testing.T, db *genji.DB) {}
	setup = func(t *testing.T, db *genji.DB) {
		t.Helper()
		q := `
CREATE TABLE test;
INSERT INTO test(a, b, c) VALUES (1, {a: 1}, [true]);
`
		err := db.Exec(q)
		assert.NoError(t, err)
	}
	tests := []struct {
		name      string
		postSetup string
	}{
		{name: "no index", postSetup: ``},
		{name: "with index", postSetup: `
CREATE INDEX ON test(a);`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			postSetup = func(t *testing.T, db *genji.DB) {
				t.Helper()

				err := db.Exec(test.postSetup)
				assert.NoError(t, err)
			}
			// --------------------------------------------------------------------------
			t.Run("wildcard", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test;`, func(t *testing.T) {
					q := `
SELECT * FROM test;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
{"a": 1.0, "b": {"a": 1.0}, "c": [true]}
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("multiple wildcards", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT *, * FROM test;`, func(t *testing.T) {
					q := `
SELECT *, * FROM test;
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
				postSetup(t, db)
				t.Run(`SELECT a, b, c FROM test;`, func(t *testing.T) {
					q := `
SELECT a, b, c FROM test;
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
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("field path, wildcards and expressions", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT a AS A, b.a + 1, * FROM test;`, func(t *testing.T) {
					q := `
SELECT a AS A, b.a + 1, * FROM test;
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
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("wildcard and other field", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT *, c FROM test;`, func(t *testing.T) {
					q := `
SELECT *, c FROM test;
`
					err := db.Exec(q)
					assert.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
				})
			})

		})
	}
}
