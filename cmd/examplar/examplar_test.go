package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	f, err := os.Open("extest1.sql")
	require.NoError(t, err)
	defer f.Close()

	ex, err := Parse(f, "extest1")
	require.NoError(t, err)

	require.Equal(t, []Line{{2, "CREATE TABLE foo (a int);"}}, ex.setup)
	require.Equal(t, []Line{{5, "DROP TABLE foo;"}}, ex.teardown)

	// first test
	example := ex.examples[0]
	require.NotNil(t, example)
	require.Equal(t, "insert something", example.Name)

	stmt := example.Statements[0]
	require.Equal(t, "INSERT INTO foo (a) VALUES (1);", stmt.Code.Text)

	stmt = example.Statements[1]
	require.Equal(t, "SELECT * FROM foo;", stmt.Code.Text)
	require.Equal(t, `{"a": 1}`, stmt.Expectation[0].Text)

	stmt = example.Statements[2]
	require.Equal(t, "SELECT a, b FROM foo;", stmt.Code.Text)
	fmt.Println("---", len(stmt.Expectation))
	require.JSONEq(t, `{"a": 1, "b": null}`, stmt.expectationText())

	stmt = example.Statements[3]
	require.Equal(t, "SELECT z FROM foo;", stmt.Code.Text)
	require.Equal(t, `{"z": null}`, stmt.Expectation[0].Text)

	// second test
	example = ex.examples[1]
	require.NotNil(t, example)
	require.Equal(t, "something else", example.Name)

	stmt = example.Statements[0]
	require.Equal(t, "INSERT INTO foo (c) VALUES (3);", stmt.Code.Text)

	stmt = example.Statements[1]
	require.Equal(t, "SELECT * FROM foo;", stmt.Code.Text)
	require.Equal(t, `{"c": 3}`, stmt.Expectation[0].Text)
}

func TestTemplate(t *testing.T) {
	g, err := os.Open("extest1_test.go.gold")
	require.NoError(t, err)
	defer g.Close()

	gb, err := ioutil.ReadAll(g)
	require.NoError(t, err)

	gold := string(gb)

	f, err := os.Open("extest1.sql")
	require.NoError(t, err)
	defer f.Close()

	ex, err := Parse(f, "extest1")
	require.NoError(t, err)

	var b strings.Builder

	err = Generate(ex, &b)
	require.NoError(t, err)

	// some code to generate the gold version
	// o, err := os.OpenFile("trace_test.go", os.O_CREATE|os.O_WRONLY, 0777)
	// require.NoError(t, err)
	// o.WriteString(b.String())
	// defer o.Close()

	require.Equal(t, strings.Split(gold, "\n"), strings.Split(b.String(), "\n"))
}
