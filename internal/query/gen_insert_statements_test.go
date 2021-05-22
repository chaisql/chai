/*
* CODE GENERATED AUTOMATICALLY WITH github.com/genjidb/genji/dev/gensqltest
* THIS FILE SHOULD NOT BE EDITED BY HAND
 */
package query_test

import (
	"regexp"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestGenInsertStatements(t *testing.T) {
	setup := func(t *testing.T, db *genji.DB) {
		t.Helper()

		q := `
CREATE TABLE test;
CREATE TABLE test_idx;
CREATE INDEX idx_a ON test_idx (a);
CREATE INDEX idx_b ON test_idx (b);
CREATE INDEX idx_c ON test_idx (c);
`
		err := db.Exec(q)
		require.NoError(t, err)
	}

	// --------------------------------------------------------------------------
	t.Run("values, no columns", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test VALUES ("a", 'b', 'c');`, func(t *testing.T) {
			q := `
INSERT INTO test VALUES ("a", 'b', 'c');
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("values, with columns", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test (a, b, c) VALUES ('a', 'b', 'c');`, func(t *testing.T) {
			q := `
INSERT INTO test (a, b, c) VALUES ('a', 'b', 'c');
SELECT pk(), * FROM test;
`
			res, err := db.Query(q)
			require.NoError(t, err)
			defer res.Close()
			raw := `
{
  "pk()": 1,
  "a": "a",
  "b": "b",
  "c":"c"
}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("values, ident", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test (a) VALUES (a);`, func(t *testing.T) {
			q := `
INSERT INTO test (a) VALUES (a);
`
			err := db.Exec(q)
			require.NotNil(t, err, "expected error, got nil")
			require.Regexp(t, regexp.MustCompile("field not found"), err.Error())
		})

	})

	// --------------------------------------------------------------------------
	t.Run("values, ident string", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test (a) VALUES (`+"`"+`a`+"`"+`);`, func(t *testing.T) {
			q := `
INSERT INTO test (a) VALUES (` + "`" + `a` + "`" + `);
`
			err := db.Exec(q)
			require.NotNil(t, err, "expected error, got nil")
			require.Regexp(t, regexp.MustCompile("field not found"), err.Error())
		})

	})

	// --------------------------------------------------------------------------
	t.Run("values, fields ident string", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test (a, `+"`"+`foo bar`+"`"+`) VALUES ('c', 'd');`, func(t *testing.T) {
			q := `
INSERT INTO test (a, ` + "`" + `foo bar` + "`" + `) VALUES ('c', 'd');
SELECT pk(), * FROM test;
`
			res, err := db.Query(q)
			require.NoError(t, err)
			defer res.Close()
			raw := `
{
  "pk()": 1,
  "a": "c",
  "foo bar": "d"
}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("values, list", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test (a, b, c) VALUES ("a", 'b', [1, 2, 3]);`, func(t *testing.T) {
			q := `
INSERT INTO test (a, b, c) VALUES ("a", 'b', [1, 2, 3]);
SELECT pk(), * FROM test;
`
			res, err := db.Query(q)
			require.NoError(t, err)
			defer res.Close()
			raw := `
{
  "pk()": 1,
  "a": "a",
  "b":"b",
  "c": [1.0, 2.0, 3.0]
}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("values, document", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test (a, b, c) VALUES ("a", 'b', {c: 1, d: c + 1});`, func(t *testing.T) {
			q := `
INSERT INTO test (a, b, c) VALUES ("a", 'b', {c: 1, d: c + 1});
SELECT pk(), * FROM test;
`
			res, err := db.Query(q)
			require.NoError(t, err)
			defer res.Close()
			raw := `
{
  "pk()": 1,
  "a": "a",
  "b": "b",
  "c": {
    "c": 1.0,
    "d": 2.0
  }
}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("document", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test VALUES {a: 'a', b: 2.3, c: 1 = 1};`, func(t *testing.T) {
			q := `
INSERT INTO test VALUES {a: 'a', b: 2.3, c: 1 = 1};
SELECT pk(), * FROM test;
`
			res, err := db.Query(q)
			require.NoError(t, err)
			defer res.Close()
			raw := `
{
  "pk()": 1,
  "a": "a",
  "b": 2.3,
  "c": true
}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("document, list", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test VALUES {a: [1, 2, 3]};`, func(t *testing.T) {
			q := `
INSERT INTO test VALUES {a: [1, 2, 3]};
SELECT pk(), * FROM test;
`
			res, err := db.Query(q)
			require.NoError(t, err)
			defer res.Close()
			raw := `
{
  "pk()": 1,
  "a": [
    1.0,
    2.0,
    3.0
  ]
}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("document, strings", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test VALUES {'a': 'a', b: 2.3};`, func(t *testing.T) {
			q := `
INSERT INTO test VALUES {'a': 'a', b: 2.3};
SELECT pk(), * FROM test;
/*result:
{
"pk()": 1,
"a": "a",
"b": 2.3
}
*/
`
			res, err := db.Query(q)
			require.NoError(t, err)
			defer res.Close()
		})

	})

	// --------------------------------------------------------------------------
	t.Run("document, double quotes", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test VALUES {"a": "b"};`, func(t *testing.T) {
			q := `
INSERT INTO test VALUES {"a": "b"};
SELECT pk(), * FROM test;
`
			res, err := db.Query(q)
			require.NoError(t, err)
			defer res.Close()
			raw := `
{
  "pk()": 1,
  "a": "b"
}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("document, references to other field", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test VALUES {a: 400, b: a * 4};`, func(t *testing.T) {
			q := `
INSERT INTO test VALUES {a: 400, b: a * 4};
SELECT pk(), * FROM test;
`
			res, err := db.Query(q)
			require.NoError(t, err)
			defer res.Close()
			raw := `
{"pk()":1,"a":400.0,"b":1600.0}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("index, values, no columns", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test_idx VALUES ("a", 'b', 'c');`, func(t *testing.T) {
			q := `
INSERT INTO test_idx VALUES ("a", 'b', 'c');
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("index, values, with columns", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test_idx (a, b, c) VALUES ('a', 'b', 'c');`, func(t *testing.T) {
			q := `
INSERT INTO test_idx (a, b, c) VALUES ('a', 'b', 'c');
SELECT pk(), * FROM test_idx;
`
			res, err := db.Query(q)
			require.NoError(t, err)
			defer res.Close()
			raw := `
{
  "pk()": 1,
  "a": "a",
  "b": "b",
  "c": "c"
}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("index, values, ident", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test_idx (a) VALUES (a);`, func(t *testing.T) {
			q := `
INSERT INTO test_idx (a) VALUES (a);
`
			err := db.Exec(q)
			require.NotNil(t, err, "expected error, got nil")
			require.Regexp(t, regexp.MustCompile("field not found"), err.Error())
		})

	})

	// --------------------------------------------------------------------------
	t.Run("index, values, ident string", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test_idx (a) VALUES (`+"`"+`a`+"`"+`);`, func(t *testing.T) {
			q := `
INSERT INTO test_idx (a) VALUES (` + "`" + `a` + "`" + `);
`
			err := db.Exec(q)
			require.NotNil(t, err, "expected error, got nil")
			require.Regexp(t, regexp.MustCompile("field not found"), err.Error())
		})

	})

	// --------------------------------------------------------------------------
	t.Run("index, values, fields ident string", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test_idx (a, `+"`"+`foo bar`+"`"+`) VALUES ('c', 'd');`, func(t *testing.T) {
			q := `
INSERT INTO test_idx (a, ` + "`" + `foo bar` + "`" + `) VALUES ('c', 'd');
SELECT pk(), * FROM test_idx;
`
			res, err := db.Query(q)
			require.NoError(t, err)
			defer res.Close()
			raw := `
{
  "pk()": 1,
  "a": "c",
  "foo bar": "d"
}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("index, values, list", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test_idx (a, b, c) VALUES ("a", 'b', [1, 2, 3]);`, func(t *testing.T) {
			q := `
INSERT INTO test_idx (a, b, c) VALUES ("a", 'b', [1, 2, 3]);
SELECT pk(), * FROM test_idx;
`
			res, err := db.Query(q)
			require.NoError(t, err)
			defer res.Close()
			raw := `
{
  "pk()": 1,
  "a": "a",
  "b":"b",
  "c": [1.0, 2.0, 3.0]
}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("index, values, document", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test_idx (a, b, c) VALUES ("a", 'b', {c: 1, d: c + 1});`, func(t *testing.T) {
			q := `
INSERT INTO test_idx (a, b, c) VALUES ("a", 'b', {c: 1, d: c + 1});
SELECT pk(), * FROM test_idx;
`
			res, err := db.Query(q)
			require.NoError(t, err)
			defer res.Close()
			raw := `
{
  "pk()": 1,
  "a": "a",
  "b": "b",
  "c": {
    "c": 1.0,
    "d": 2.0
  }
}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("index, document", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test_idx VALUES {a: 'a', b: 2.3, c: 1 = 1};`, func(t *testing.T) {
			q := `
INSERT INTO test_idx VALUES {a: 'a', b: 2.3, c: 1 = 1};
SELECT pk(), * FROM test_idx;
`
			res, err := db.Query(q)
			require.NoError(t, err)
			defer res.Close()
			raw := `
{
  "pk()": 1,
  "a": "a",
  "b": 2.3,
  "c": true
}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("index, document, list", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test_idx VALUES {a: [1, 2, 3]};`, func(t *testing.T) {
			q := `
INSERT INTO test_idx VALUES {a: [1, 2, 3]};
SELECT pk(), * FROM test_idx;
`
			res, err := db.Query(q)
			require.NoError(t, err)
			defer res.Close()
			raw := `
{
  "pk()": 1,
  "a": [
    1.0,
    2.0,
    3.0
  ]
}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("index, document, strings", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test_idx VALUES {'a': 'a', b: 2.3};`, func(t *testing.T) {
			q := `
INSERT INTO test_idx VALUES {'a': 'a', b: 2.3};
SELECT pk(), * FROM test_idx;
/*result:
{
"pk()": 1,
"a": "a",
"b": 2.3
}
*/
`
			res, err := db.Query(q)
			require.NoError(t, err)
			defer res.Close()
		})

	})

	// --------------------------------------------------------------------------
	t.Run("index, document, double quotes", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test_idx VALUES {"a": "b"};`, func(t *testing.T) {
			q := `
INSERT INTO test_idx VALUES {"a": "b"};
SELECT pk(), * FROM test_idx;
`
			res, err := db.Query(q)
			require.NoError(t, err)
			defer res.Close()
			raw := `
{
  "pk()": 1,
  "a": "b"
}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("index, document, references to other field", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test_idx VALUES {a: 400, b: a * 4};`, func(t *testing.T) {
			q := `
INSERT INTO test_idx VALUES {a: 400, b: a * 4};
SELECT pk(), * FROM test_idx;
`
			res, err := db.Query(q)
			require.NoError(t, err)
			defer res.Close()
			raw := `
{"pk()":1,"a":400.0,"b":1600.0}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("read-only tables", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO __genji_tables VALUES {a: 400, b: a * 4};`, func(t *testing.T) {
			q := `
INSERT INTO __genji_tables VALUES {a: 400, b: a * 4};
`
			err := db.Exec(q)
			require.NotNil(t, err, "expected error, got nil")
			require.Regexp(t, regexp.MustCompile("cannot write to read-only table"), err.Error())
		})

	})

	// --------------------------------------------------------------------------
	t.Run("insert with primary keys", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE testpk (foo INTEGER PRIMARY KEY);`, func(t *testing.T) {
			q := `
CREATE TABLE testpk (foo INTEGER PRIMARY KEY);
INSERT INTO testpk (bar) VALUES (1);
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

		t.Run(`INSERT INTO testpk (bar, foo) VALUES (1, 2);`, func(t *testing.T) {
			q := `
INSERT INTO testpk (bar, foo) VALUES (1, 2);
INSERT INTO testpk (bar, foo) VALUES (1, 2);
`
			err := db.Exec(q)
			require.NotNil(t, err, "expected error, got nil")
			require.Regexp(t, regexp.MustCompile("duplicate"), err.Error())
		})

	})

	// --------------------------------------------------------------------------
	t.Run("insert with shadowing", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO test (`+"`"+`pk()`+"`"+`, `+"`"+`key`+"`"+`) VALUES (1, 2);`, func(t *testing.T) {
			q := `
INSERT INTO test (` + "`" + `pk()` + "`" + `, ` + "`" + `key` + "`" + `) VALUES (1, 2);
`
			res, err := db.Query(q)
			require.NoError(t, err)
			defer res.Close()
		})

	})

	// --------------------------------------------------------------------------
	t.Run("insert with types constraints", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_tc(`, func(t *testing.T) {
			q := `
CREATE TABLE test_tc(
b bool, db double,
i integer, bb blob, byt bytes,
t text, a array, d document
);
INSERT INTO test_tc
VALUES {
i: 10000000000, db: 21.21, b: true,
bb: "YmxvYlZhbHVlCg==", byt: "Ynl0ZXNWYWx1ZQ==",
t: "text", a: [1, "foo", true], d: {"foo": "bar"}
};
SELECT * FROM test_tc;
`
			res, err := db.Query(q)
			require.NoError(t, err)
			defer res.Close()
			raw := `
{
  "i": 10000000000,
  "db": 21.21,
  "b": true,
  "bb": CAST("YmxvYlZhbHVlCg==" AS BLOB),
  "byt": CAST("Ynl0ZXNWYWx1ZQ==" AS BYTES),
  "t": "text",
  "a": [
    1.0,
    "foo",
    true
  ],
  "d": {
    "foo": "bar"
  }
}
`
			testutil.RequireStreamEq(t, raw, res)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("insert with inferred constraints", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_ic(a INTEGER, s.b TEXT);`, func(t *testing.T) {
			q := `
CREATE TABLE test_ic(a INTEGER, s.b TEXT);
INSERT INTO test_ic VALUES {s: 1};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

}
