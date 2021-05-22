package shell

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/cmd/genji/dbutil"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine/badgerengine"
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
			require.NoError(t, err)
			defer db.Close()

			for _, tb := range test.tables {
				err := db.Exec("CREATE TABLE " + tb)
				require.NoError(t, err)
			}

			var buf bytes.Buffer
			err = runTablesCmd(db, &buf)
			require.NoError(t, err)

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
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec(`
				CREATE TABLE foo;
				CREATE INDEX idx_foo_a ON foo (a);
				CREATE INDEX idx_foo_b ON foo (b);
				CREATE TABLE bar;
				CREATE INDEX idx_bar_a_b ON bar (a, b);
			`)
			require.NoError(t, err)

			var buf bytes.Buffer
			err = runIndexesCmd(db, test.tableName, &buf)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.want, buf.String())
			}
		})
	}
}

func TestSaveCommand(t *testing.T) {
	dir, err := ioutil.TempDir("", "genji")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	tests := []struct {
		engine string
		path   string
	}{
		{"bolt", filepath.Join(dir, "/test.db")},
		{"badger", filepath.Join(dir, "/badger")},
	}

	for _, tt := range tests {
		t.Cleanup(func() {
			os.RemoveAll(tt.path)
		})

		t.Run(tt.engine+"/OK", func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec(`
				CREATE TABLE test (a DOUBLE);
				CREATE INDEX idx_a_b ON test (a, b);
			`)
			require.NoError(t, err)
			err = db.Exec("INSERT INTO test (a, b) VALUES (?, ?)", 1, 2)
			require.NoError(t, err)
			err = db.Exec("INSERT INTO test (a, b) VALUES (?, ?)", 2, 2)
			require.NoError(t, err)
			err = db.Exec("INSERT INTO test (a, b) VALUES (?, ?)", 3, 2)
			require.NoError(t, err)

			// save the dummy database
			err = runSaveCmd(context.Background(), db, tt.engine, tt.path)
			require.NoError(t, err)

			if tt.engine == "badger" {
				ng, err := badgerengine.NewEngine(badger.DefaultOptions(tt.path).WithLogger(nil))
				require.NoError(t, err)
				db, err = genji.New(context.Background(), ng)
				require.NoError(t, err)
			} else {
				db, err = genji.Open(tt.path)
				require.NoError(t, err)
			}
			defer db.Close()

			// ensure that the data is present
			doc, err := db.QueryDocument("SELECT * FROM test")
			require.NoError(t, err)

			var res struct {
				A int
				B int
			}
			err = document.StructScan(doc, &res)
			require.NoError(t, err)

			require.Equal(t, 1, res.A)
			require.Equal(t, 2, res.B)

			// ensure that the index has been created
			indexes, err := dbutil.ListIndexes(context.Background(), db, "")
			require.NoError(t, err)
			require.Len(t, indexes, 1)
			require.Equal(t, "idx_a_b", indexes[0])
		})
	}
}
