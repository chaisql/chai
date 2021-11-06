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

func TestWhere(t *testing.T) {
	setup := func(t *testing.T, db *genji.DB) {}
	postSetup := func(t *testing.T, db *genji.DB) {}
	setup = func(t *testing.T, db *genji.DB) {
		t.Helper()
		q := `
CREATE TABLE test(
id int primary key,
a int,
b double,
c bool,
d text,
e blob,
f.a int, -- f document
g[0] int -- e array
);
INSERT INTO test VALUES
{
id: 1,
a: 10,
b: 1.0,
c: false,
d: "a",
e: "\xaa",
f: {a: 1},
g: [1]
},
{
id: 2,
a: 20,
b: 2.0,
c: true,
d: "b",
e: "\xab",
f: {a: 2},
g: [2]
},
{
id: 3,
a: 30,
b: 3.0,
c: false,
d: "c",
e: "\xac",
f: {a: 3},
g: [3]
},
{
id: 4,
a: 40,
b: 4.0,
c: true,
d: "d",
e: "\xad",
f: {a: 4},
g: [4]
};
`
		err := db.Exec(q)
		assert.NoError(t, err)
	}
	tests := []struct {
		name      string
		postSetup string
	}{
		{name: "no index", postSetup: ``},
		{name: "index on a", postSetup: `
CREATE INDEX ON test(a);`},
		{name: "index on b", postSetup: `
CREATE INDEX ON test(b);`},
		{name: "index on c", postSetup: `
CREATE INDEX ON test(c);`},
		{name: "index on d", postSetup: `
CREATE INDEX ON test(d);`},
		{name: "index on e", postSetup: `
CREATE INDEX ON test(e);`},
		{name: "index on f", postSetup: `
CREATE INDEX ON test(f);`},
		{name: "index on g", postSetup: `
CREATE INDEX ON test(g);`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			postSetup = func(t *testing.T, db *genji.DB) {
				t.Helper()

				err := db.Exec(test.postSetup)
				assert.NoError(t, err)
			}
			// --------------------------------------------------------------------------
			t.Run("pk =", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE id = 1;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE id = 1;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("pk !=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE id != 1;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE id != 1;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("pk >", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE id > 1;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE id > 1;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("pk >=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE id >= 1;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE id >= 1;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("pk <", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE id < 3;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE id < 3;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("pk <=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE id <= 3;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE id <= 3;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("pk IN", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE id IN (1, 3);`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE id IN (1, 3);
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("pk NOT IN", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE id NOT IN (1, 3);`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE id NOT IN (1, 3);
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("int =", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE a = 10;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE a = 10;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("int !=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE a != 10;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE a != 10;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("int >", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE a > 10;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE a > 10;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("int >=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE a >= 10;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE a >= 10;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("int <", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE a < 30;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE a < 30;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("int <=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE a <= 30;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE a <= 30;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("int IN", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE a IN (10, 30);`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE a IN (10, 30);
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("int NOT IN", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE a NOT IN (10, 30);`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE a NOT IN (10, 30);
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("double =", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE b = 1.0;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE b = 1.0;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("double !=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE b != 1.0;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE b != 1.0;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("double >", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE b > 1.0;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE b > 1.0;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("double >=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE b >= 1.0;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE b >= 1.0;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("double <", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE b < 3.0;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE b < 3.0;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("double <=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE b <= 3.0;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE b <= 3.0;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("double IN", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE b IN (1.0, 3.0);`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE b IN (1.0, 3.0);
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("double NOT IN", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE b NOT IN (1.0, 3.0);`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE b NOT IN (1.0, 3.0);
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("bool =", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE c = true;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE c = true;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, true)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("bool !=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE c != true;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE c != true;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
`
					testutil.RequireStreamEq(t, raw, res, true)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("bool >", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE c > false;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE c > false;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, true)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("bool >=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE c >= false;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE c >= false;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, true)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("bool <", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE c < true;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE c < true;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
`
					testutil.RequireStreamEq(t, raw, res, true)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("bool <=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE c <= true;`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE c <= true;
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, true)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("bool IN", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE c IN (true, false);`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE c IN (true, false);
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, true)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("bool NOT IN", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE c NOT IN (true, 3);`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE c NOT IN (true, 3);
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
`
					testutil.RequireStreamEq(t, raw, res, true)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("text =", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE d = "a";`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE d = "a";
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("text !=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE d != "a";`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE d != "a";
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("text >", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE d > "a";`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE d > "a";
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("text >=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE d >= "a";`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE d >= "a";
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("text <", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE d < "c";`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE d < "c";
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("text <=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE d <= "c";`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE d <= "c";
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("text IN", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE d IN ("a", "c");`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE d IN ("a", "c");
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("text NOT IN", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE d NOT IN ("a", "c");`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE d NOT IN ("a", "c");
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("blob =", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE e = "\xaa";`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE e = "\xaa";
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("blob !=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE e != "\xaa";`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE e != "\xaa";
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("blob >", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE e > "\xaa";`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE e > "\xaa";
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("blob >=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE e >= "\xaa";`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE e >= "\xaa";
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("blob <", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE e < "\xac";`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE e < "\xac";
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("blob <=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE e <= "\xac";`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE e <= "\xac";
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("blob IN", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE e IN ("\xaa", "\xac");`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE e IN ("\xaa", "\xac");
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("blob NOT IN", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE e NOT IN ("\xaa", "\xac");`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE e NOT IN ("\xaa", "\xac");
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("doc =", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE f = {a: 1};`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE f = {a: 1};
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("doc !=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE f != {a: 1};`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE f != {a: 1};
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("doc >", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE f > {a: 1};`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE f > {a: 1};
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("doc >=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE f >= {a: 1};`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE f >= {a: 1};
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("doc <", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE f < {a: 3};`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE f < {a: 3};
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("doc <=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE f <= {a: 3};`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE f <= {a: 3};
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("doc IN", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE f IN ({a: 1}, {a: 3});`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE f IN ({a: 1}, {a: 3});
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("doc NOT IN", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE f NOT IN ({a: 1}, {a: 3});`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE f NOT IN ({a: 1}, {a: 3});
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("array =", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE g = [1];`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE g = [1];
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("array !=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE g != [1];`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE g != [1];
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("array >", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE g > [1];`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE g > [1];
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("array >=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE g >= [1];`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE g >= [1];
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("array <", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE g < [3];`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE g < [3];
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("array <=", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE g <= [3];`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE g <= [3];
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("array IN", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE g IN ([1], [3]);`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE g IN ([1], [3]);
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

			// --------------------------------------------------------------------------
			t.Run("array NOT IN", func(t *testing.T) {
				db, err := genji.Open(":memory:")
				assert.NoError(t, err)
				defer db.Close()

				setup(t, db)
				postSetup(t, db)
				t.Run(`SELECT * FROM test WHERE g NOT IN ([1], [3]);`, func(t *testing.T) {
					q := `
SELECT * FROM test WHERE g NOT IN ([1], [3]);
`
					res, err := db.Query(q)
					assert.NoError(t, err)
					defer res.Close()
					raw := `
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4]
    }
`
					testutil.RequireStreamEq(t, raw, res, false)
				})
			})

		})
	}
}
