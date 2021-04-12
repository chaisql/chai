# Examplar

A tool to generate tests from on an annotated SQL file that specifies an example and its expected result.

A file named `selecting.sql`

```sql
--- setup:
CREATE TABLE foo;
--- teardown:
DROP TABLE foo;
--- test: insert something
INSERT INTO foo (a) VALUES (1);
SELECT * FROM foo;
--- `[{"a": 1}]`
```

Gets transformed into (abbreviated):

```go
func TestSelecting(t *testing.T) {
    db, err := genji.Open(":memory:")

    teardown := func() {
       db.Exec("DROP TABLE foo;")
    }

    setup := func() {
        err = db.Exec("CREATE TABLE foo (a int);")
    }

    t.Run("insert something", func(t *testing.T) {
        t.Cleanup(teardown)
        setup()

        res, err := db.Query(
          "INSERT INTO foo (a) VALUES (1);" +
          "SELECT * FROM foo;"
        )
        require.NoError(t, err)
        defer res.Close()

        data := jsonResult(t, res)

        expected = `[{"a": 1}]`
        require.JSONEq(t, expected, string(data))
    }
}
```

## Usage

To populate tests that would be in an `integration` package, create a go file in folder named `integration` with the following code:

```go
package integration

//go:generate examplar -package=integration fixtures/sql/*.sql
```

Assuming `fixtures/sql/` contains the following files:

```
extest1.sql
extest2.sql
```

Running `go generate` will generate these tests files in the `integration` folder:

```
extest1_test.go
extest2_test.go
```

Those tests files can be run like any normal tests.

## How it works

Examplar reads a SQL file, looking either for raw text or annotations that specifies what those lines are supposed to do from a testing perpective.

Once a SQL file has been parsed, it generates a `(...)_test.go` file that looks similar to the handwritten test that would have been written.

Every original line in the input SQL file is executed and expected to not return any error.

Those test files have no dependencies on Examplar or the SQL file that has been used to generate the test.

### Annotations

Annotations starts with `---` (the SQL comment `--` and an additional `-`) and can
be followed by a keyword specified in the list below followed by a `:` or special symbols to pass data to set expectations.

- `setup:`

  - all lines up to the next annotation are to be considered as one single statement for the setup block.

- `teardown:`
  - all lines up to the next annotation are to be considered as single statement for the teardown block.

:bulb: `setup` and `teardown` blocks will generate code being ran around **each indidual** `test` block.
They are optional and can be declared in no particular order, but there can only be one of each.

- `test: [TEST NAME]`
  - a test is composed of one or many statements; a statement composed of one or multiple lines and is terminated by an expectation (see below).

:bulb: Each `test` block will generate an individual `t.Run("[TEST NAME]", ...)` function. At least one test block must be present.

- `` `[JSON]` ``

  - the statement above this annotation will be compared to `[JSON]` when evaluated at the runtime.
  - Invalid JSON won't yield an error ar generate time, but the generated test will always fail at runtime.

- ` ``` `
  - the statement above this annotation will be compared to `[JSON]` when evaluated at the runtime.
  - all the following lines until another triple backtick annoattion is found are to be considered as part of a single multiline JSON data.
  - indentation will be preserved in the generating test file for readablilty. Similarly, invalid JSON will only yield an error when the resulting test is evaluated.

## Goals and non-goals

Examplar objective is to provide a clear and simple way to write example of Genji SQL code and its expected results. It has to be easy enough for anyone to edit or write an example SQL file without having to read the present documentation. If this objective succeeds, it opens the path Examplar SQL files being used by users and contributors to showcase a bug or a feature request in a Github issue.

By being totally independent from Genji itself for the parsing part, it frees itself from needing to be updated when something changes under the hood and allows to keep the code as merely parsing textual data. In other words, if anything changes in Genji on how to execute queries, only the template needs to be updated.

## Limitations

A breaking change in the API has only
For now, let's observe how useful Examplar can and what we can make out of it.
Then we can then see if it's worth addressing the following limitations:

- Expecting an error instead of JSON is not supported.
- Error messages on failed expectations do not reference the orignal file directly, which could be useful on complex examples sql files.
