package dbutil

import (
	"bytes"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/chaisql/chai"
	"github.com/stretchr/testify/require"
)

func TestExecSQL(t *testing.T) {
	db, err := sql.Open("chai", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var got bytes.Buffer
	err = ExecSQL(t.Context(), db, strings.NewReader(`
		CREATE TABLE test(a INT, b INT);
		CREATE INDEX idx_a ON test (a);
		INSERT INTO test (a, b) VALUES (1, 2), (2, 2), (3, 2);
		SELECT * FROM test;
	`), &got)
	require.NoError(t, err)

	require.Equal(t, "[\n  1,\n  2\n]\n[\n  2,\n  2\n]\n[\n  3,\n  2\n]\n", got.String())

	var res struct {
		A int
		B int
	}

	// Ensure that the data is present.
	err = db.QueryRow("SELECT * FROM test").Scan(&res.A, &res.B)
	require.NoError(t, err)
	require.Equal(t, 1, res.A)
	require.Equal(t, 2, res.B)
}
