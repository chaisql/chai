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
		CREATE TABLE test(a INT, b TEXT);
		CREATE INDEX idx_a ON test (a);
		BEGIN;
		INSERT INTO test (a, b) VALUES (10, 'aa'), (20, 'bb'), (30, 'cc');
		ROLLBACK;
		BEGIN;
		INSERT INTO test (a, b) VALUES (1, 'a'), (2, 'b'), (3, 'c');
		SELECT * FROM test;
		COMMIT;
		SELECT b, a FROM test;
	`), &got)
	require.NoError(t, err)

	require.Equal(t, "a|b\n1|\"a\"\n2|\"b\"\n3|\"c\"\n\nb|a\n\"a\"|1\n\"b\"|2\n\"c\"|3\n", got.String())

	var res struct {
		A int
		B string
	}

	// Ensure that the data is present.
	err = db.QueryRow("SELECT * FROM test").Scan(&res.A, &res.B)
	require.NoError(t, err)
	require.Equal(t, 1, res.A)
	require.Equal(t, "a", res.B)
}
