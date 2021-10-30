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

func TestAggregation(t *testing.T) {
	setup := func(t *testing.T, db *genji.DB) {
		t.Helper()

		q := `
CREATE TABLE foo(a int);
INSERT INTO foo (a) VALUES (1), (2), (3), (4), (5);
`
		err := db.Exec(q)
		assert.NoError(t, err)
	}

	// --------------------------------------------------------------------------
	t.Run("GROUP BY a", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`SELECT a FROM foo GROUP BY a`, func(t *testing.T) {
			q := `
SELECT a FROM foo GROUP BY a
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"a": 1}
{"a": 2}
{"a": 3}
{"a": 4}
{"a": 5}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("GROUP BY a % 2", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		assert.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`SELECT a % 2 FROM foo GROUP BY a % 2`, func(t *testing.T) {
			q := `
SELECT a % 2 FROM foo GROUP BY a % 2
`
			res, err := db.Query(q)
			assert.NoError(t, err)
			defer res.Close()
			raw := `
{"a % 2": 0}
{"a % 2": 1}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

}
