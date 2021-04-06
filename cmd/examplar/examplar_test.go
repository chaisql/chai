package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	f, err := os.Open("extest1.sql")
	require.NoError(t, err)

	ex, err := parse(f)
	require.NoError(t, err)

	require.Equal(t, ex.setup, []string{"CREATE TABLE foo (a int);"})
	require.Equal(t, ex.teardown, []string{"DROP TABLE foo;"})

	example := ex.examples[0]
	require.NotNil(t, example)
	require.Equal(t, example.name, "insert something")

	require.Equal(t, example.statements, []string{"INSERT INTO foo (1);"})
	require.Equal(t, example.assertions, []string{"1"})
}
