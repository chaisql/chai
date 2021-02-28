package main

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/cmd/genji/shell"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/stretchr/testify/require"
)

func TestExecuteRestore(t *testing.T) {
	tests := []struct {
		name   string
		engine string
		path   string
		target string
		fail   bool
	}{
		{"without engine", "", os.TempDir() + "/test.db", os.TempDir() + "restored.db", true},
		{"error with bad engine", "test", os.TempDir(), os.TempDir() + "restored.db", true},
		{"error without db path", "bolt", "", os.TempDir(), true},
		{"with bolt", "bolt", os.TempDir() + "/test.db", os.TempDir() + "restored.db", false},
		{"with badger", "badger", os.TempDir(), os.TempDir(), false},
	}

	for _, tt := range tests {
		t.Cleanup(func() {
			os.RemoveAll(tt.path)
			os.RemoveAll(tt.target)
		})

		t.Run(tt.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec(`CREATE TABLE test (a);
					CREATE INDEX idx_a ON test (a);`)
			require.NoError(t, err)

			err = db.Exec(`INSERT INTO test (a, b) VALUES (?, ?)`, 1, 2)
			require.NoError(t, err)
			err = db.Exec("INSERT INTO test (a, b) VALUES (?, ?)", 2, 2)
			require.NoError(t, err)
			err = db.Exec("INSERT INTO test (a, b) VALUES (?, ?)", 3, 2)
			require.NoError(t, err)

			// Create a dump.
			var buf bytes.Buffer
			err = shell.RunDumpCmd(db, &buf, []string{"test"})
			require.NoError(t, err)
			
			// Use the dump.
			r := &buf
			err = executeRestore(context.Background(), r, tt.engine, tt.target)
			if tt.fail {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			if tt.engine == "badger" {
				ng, err := badgerengine.NewEngine(badger.DefaultOptions(tt.target).WithLogger(nil))
				require.NoError(t, err)
				db, err = genji.New(context.Background(), ng)
				require.NoError(t, err)
			} else {
				db, err = genji.Open(tt.target)
				require.NoError(t, err)
			}
			defer db.Close()

			// Ensure that the data is present.
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

			// Ensure that the index has been created.
			err = db.View(func(tx *genji.Tx) error {
				indexes := tx.ListIndexes()
				require.Len(t, indexes, 1)
				require.Equal(t, "idx_a", indexes[0])

				return nil
			})
			require.NoError(t, err)

			// ensure that the data has been reindexed
			tx, err := db.Begin(false)
			require.NoError(t, err)
			defer tx.Rollback()

			idx, err := tx.GetIndex("idx_a")
			require.NoError(t, err)

			// check that by iterating through the index and finding the previously inserted values
			var i int
			err = idx.AscendGreaterOrEqual(document.Value{Type: document.DoubleValue}, func(v, k []byte) error {
				i++
				return nil
			})

			require.Equal(t, 3, i)
			require.NoError(t, err)
		})
	}
}
