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

	ex, err := parse(f, "extest1")
	require.NoError(t, err)

	require.Equal(t, ex.setup, []string{"CREATE TABLE foo (a int);"})
	require.Equal(t, ex.teardown, []string{"DROP TABLE foo;"})

	example := ex.examples[0]
	require.NotNil(t, example)
	require.Equal(t, example.name, "insert something")

	stmt := example.statements[0]
	require.Equal(t, stmt.Code, "INSERT INTO foo (a) VALUES (1);")

	stmt = example.statements[1]
	require.Equal(t, stmt.Code, "SELECT * FROM foo;")
	require.Equal(t, stmt.EqAssertion, `{"a": 1}`)
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

	ex, err := parse(f, "extest1")
	require.NoError(t, err)

	var b strings.Builder

	err = generate(ex, &b)
	require.NoError(t, err)

	fmt.Printf(b.String() + "\n")
	require.Equal(t, strings.Split(gold, "\n"), strings.Split(b.String(), "\n"))
}
