package main

import (
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

	require.Equal(t, ex.setup, []string{"CREATE TABLE foo (a int);"})
	require.Equal(t, ex.teardown, []string{"DROP TABLE foo;"})

	example := ex.examples[0]
	require.NotNil(t, example)
	require.Equal(t, "insert something", example.name)

	stmt := example.statements[0]
	require.Equal(t, "INSERT INTO foo (a) VALUES (1);", stmt.Code)

	stmt = example.statements[1]
	require.Equal(t, "SELECT * FROM foo;", stmt.Code)
	require.Equal(t, `{"a": 1}`, stmt.Expectation)

	stmt = example.statements[2]
	require.Equal(t, "SELECT a, b FROM foo;", stmt.Code)
	require.JSONEq(t, `{"a": 1, "b": null}`, stmt.Expectation)
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

	require.Equal(t, strings.Split(gold, "\n"), strings.Split(b.String(), "\n"))
}
