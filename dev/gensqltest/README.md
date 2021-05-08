# GenSQLTest

A tool to generate tests from on an annotated SQL file, with queries and their expected results.

A file named `selecting.sql`

```sql
-- setup:
CREATE TABLE foo (a int);
CREATE TABLE bar;

-- test: insert something
INSERT INTO foo (a) VALUES (1);

SELECT * FROM foo;
/* result:
{
  "a": 1
}
*/
```

Gets transformed into (abbreviated):

```go
func TestSomething(t *testing.T) {
	setup := func(t *testing.T, db *genji.DB) {
		q := `
CREATE TABLE foo (a int);
CREATE TABLE bar;
`
		err := db.Exec(q)
		require.NoError(t, err)
	}

	// --------------------------------------------------------------------------
	t.Run("insert something", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`INSERT INTO foo (a) VALUES (1);`, func(t *testing.T) {
			q := `
INSERT INTO foo (a) VALUES (1);
SELECT * FROM foo;
`
			res, err := db.Query(q)
			require.NoError(t, err)
			defer res.Close()
			raw := `
{
  "a": 1
}
`
			testutil.RequireStreamEq(t, raw, res)
        })
	})
}
```

## Usage

To populate tests that would be in an `query_test` package, create a go file named `gentests.go` in the `query` package with the following code:

```go
package query

//go:generate go run ../dev/gensqltest -package=query_test ./*_test.sql
```

Every test is ran in isolation, with a newly created memory database, with the setup block run on it.
If multiple results or errors are expected within a test, they share the same database.

### Annotations

- `-- setup:`

  - all lines up to the next annotation are to be considered as one single statement for the setup block.

- `-- test: [TEST NAME]`
  - a test is composed of one or many statements; a statement composed of one or multiple lines and is terminated by an expectation (see below).

:bulb: Each `test` block will generate an individual `t.Run("[TEST NAME]", ...)` function. At least one test block must be present.

- `/* result: `

  - all lines until the end of that comment are considered to be the expected output

- `- error: [REGEXP]`
  - expect the above statements to raise an error, that matches `[REPEXP]`
  - expecting any error can be achieved by giving a blank `[REGEXP]` as in `-error:`
