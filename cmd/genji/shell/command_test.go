package shell

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/cmd/genji/dbutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/stretchr/testify/require"
)

func TestRunTablesCmd(t *testing.T) {
	tests := []struct {
		name   string
		tables []string
		want   string
	}{
		{
			"No table",
			nil,
			"",
		},
		{
			"With tables",
			[]string{"foo", "bar"},
			"bar\nfoo\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			assert.NoError(t, err)
			defer db.Close()

			for _, tb := range test.tables {
				err := db.Exec("CREATE TABLE " + tb)
				assert.NoError(t, err)
			}

			var buf bytes.Buffer
			err = runTablesCmd(db, &buf)
			assert.NoError(t, err)

			require.Equal(t, test.want, buf.String())
		})
	}
}

func TestIndexesCmd(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		want      string
		fails     bool
	}{
		{"All", "", "idx_bar_a_b\nidx_foo_a\nidx_foo_b\n", false},
		{"With table name", "foo", "idx_foo_a\nidx_foo_b\n", false},
		{"With nonexistent table name", "baz", "", true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			assert.NoError(t, err)
			defer db.Close()

			err = db.Exec(`
				CREATE TABLE foo(a, b);
				CREATE INDEX idx_foo_a ON foo (a);
				CREATE INDEX idx_foo_b ON foo (b);
				CREATE TABLE bar(a, b);
				CREATE INDEX idx_bar_a_b ON bar (a, b);
			`)
			assert.NoError(t, err)

			var buf bytes.Buffer
			err = runIndexesCmd(db, test.tableName, &buf)
			if test.fails {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				require.Equal(t, test.want, buf.String())
			}
		})
	}
}

func TestSaveCommand(t *testing.T) {
	dir, err := ioutil.TempDir("", "genji")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := genji.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	err = db.Exec(`
		CREATE TABLE test (a DOUBLE, b, ...);
		CREATE INDEX idx_a_b ON test (a, b);
	`)
	assert.NoError(t, err)
	err = db.Exec("INSERT INTO test (a, b) VALUES (?, ?)", 1, 2)
	assert.NoError(t, err)
	err = db.Exec("INSERT INTO test (a, b) VALUES (?, ?)", 2, 2)
	assert.NoError(t, err)
	err = db.Exec("INSERT INTO test (a, b) VALUES (?, ?)", 3, 2)
	assert.NoError(t, err)

	// save the dummy database
	err = runSaveCmd(context.Background(), db, dir)
	assert.NoError(t, err)

	db, err = genji.Open(dir)
	assert.NoError(t, err)
	defer db.Close()

	// ensure that the data is present
	r, err := db.QueryRow("SELECT * FROM test")
	assert.NoError(t, err)

	var res struct {
		A int
		B int
	}
	err = r.StructScan(&res)
	assert.NoError(t, err)

	require.Equal(t, 1, res.A)
	require.Equal(t, 2, res.B)

	// ensure that the index has been created
	indexes, err := dbutil.ListIndexes(db, "")
	assert.NoError(t, err)
	require.Len(t, indexes, 1)
	require.Equal(t, "idx_a_b", indexes[0])
}
