package dbutil

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/chaisql/chai"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/stretchr/testify/require"
)

func TestExecSQL(t *testing.T) {
	db, err := chai.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	var got bytes.Buffer
	err = ExecSQL(context.Background(), db, strings.NewReader(`
		CREATE TABLE test(a INT, b INT);
		CREATE INDEX idx_a ON test (a);
		INSERT INTO test (a, b) VALUES (1, 2), (2, 2), (3, 2);
		SELECT * FROM test;
	`), &got)
	assert.NoError(t, err)

	require.Equal(t, "{\n  \"a\": 1,\n  \"b\": 2\n}\n{\n  \"a\": 2,\n  \"b\": 2\n}\n{\n  \"a\": 3,\n  \"b\": 2\n}\n", got.String())

	// Ensure that the data is present.
	row, err := db.QueryRow("SELECT * FROM test")
	assert.NoError(t, err)

	var res struct {
		A int
		B int
	}
	err = row.StructScan(&res)
	assert.NoError(t, err)
	require.Equal(t, 1, res.A)
	require.Equal(t, 2, res.B)
}
