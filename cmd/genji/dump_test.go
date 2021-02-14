package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/cmd/genji/shell"
	"github.com/stretchr/testify/require"
)

func TestExecuteDump(t *testing.T) {
	// db to save.
	testDb := os.TempDir() + "test.db"
	file := os.TempDir() + "dump.sql"
	tests := []struct {
		name   string
		tables []string
		file   string
		engine string
		dbPath string
		fails  bool
	}{
		{"Dump errored with no option", []string{}, ``, "bolt", ``, true},
		{"Dump errored with bad engine", []string{"test", "foo"}, file, "test", testDb, true},
		{"Dump to stdout", []string{"test"}, ``, "bolt", testDb, false},
		{"Dump to stdout list of tables", []string{"test", "foo"}, ``, "bolt", testDb, false},
		{"Dump to stdout list of tables with badger", []string{"test", "foo"}, ``, "badger", os.TempDir(), false},
		{"Dump in a file", []string{"test", "foo"}, file, "bolt", testDb, false},
		{"Dump in a file with badger", []string{"test", "foo"}, file, "badger", os.TempDir(), false},
	}

	for _, tt := range tests {
		t.Cleanup(func() {
			_ = os.RemoveAll(tt.dbPath)
			_ = os.RemoveAll(testDb)
		})

		t.Run(tt.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)

			defer db.Close()
			var b bytes.Buffer
			tx := "BEGIN TRANSACTION;\n"
			b.WriteString(tx)

			for i, table := range tt.tables {
				if i > 0 {
					b.WriteString("\n")
				}

				q := fmt.Sprintf("CREATE TABLE %s (\n a \n);", table)
				err = db.Exec(q)
				require.NoError(t, err)
				b.WriteString(q + "\n")

				q = fmt.Sprintf(`CREATE INDEX idx_a_%s ON %s (a);`, table, table)
				err = db.Exec(q)
				require.NoError(t, err)
				b.WriteString(q + "\n")

				q = fmt.Sprintf(`INSERT INTO %s VALUES {"a": %d, "b": %d};`, table, 1, 2)
				err = db.Exec(q)
				require.NoError(t, err)
				b.WriteString(q + "\n")

				q = fmt.Sprintf(`INSERT INTO %s VALUES {"a": %d, "b": %d};`, table, 2, 2)
				err = db.Exec(q)
				require.NoError(t, err)
				b.WriteString(q + "\n")

				q = fmt.Sprintf(`INSERT INTO %s VALUES {"a": %d, "b": %d};`, table, 3, 2)
				err = db.Exec(q)
				require.NoError(t, err)
				b.WriteString(q + "\n")
			}

			if tt.dbPath != "" {
				testDb = tt.dbPath
			}

			engine := tt.engine
			if tt.engine != "bolt" && tt.engine != "badger" {
				engine = "bolt"
			}

			err = shell.RunSaveCmd(context.Background(), db, engine, testDb)
			require.NoError(t, err)

			var buf bytes.Buffer
			err = executeDump(context.Background(), tt.file, tt.tables, tt.engine, tt.dbPath, &buf)

			if tt.file != "" {
				b, err := ioutil.ReadFile(tt.file)
				require.NoError(t, err)
				buf.Write(b)
			}

			if tt.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			ci := "COMMIT;\n"
			b.WriteString(ci)

			require.Equal(t, b.String(), buf.String())
		})

	}
}
