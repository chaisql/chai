package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/cmd/genji/shell"
	"github.com/stretchr/testify/require"
)

func TestExecuteDump(t *testing.T) {
	// db to save.
	saveDB := os.TempDir() + "/save.db"
	tests := []struct {
		name   string
		tables []string
		engine string
		dbPath string
		fails  bool
	}{
		{"Dump errored with no option", []string{}, "bolt", ``, true},
		{"Dump errored with bad engine", []string{"test", "foo"}, "test", saveDB, true},
		{"Dump to stdout", []string{"test"}, "bolt", saveDB, false},
		{"Dump to stdout list of tables", []string{"test", "foo"}, "bolt", saveDB, false},
		{"Dump to stdout list of tables with badger", []string{"test", "foo"}, "badger", os.TempDir() + "/tmp", false},
		{"Dump in a file", []string{"test", "foo"}, "bolt", saveDB, false},
	}

	for _, tt := range tests {
		t.Cleanup(func() {
			_ = os.RemoveAll(saveDB)
			_ = os.RemoveAll(tt.dbPath)
		})

		t.Run(tt.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)

			var b bytes.Buffer
			b.WriteString("BEGIN TRANSACTION;\n")

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
			b.WriteString("COMMIT;\n")

			if tt.dbPath != "" {
				saveDB = tt.dbPath
			}

			engine := tt.engine
			if tt.engine != "bolt" && tt.engine != "badger" {
				engine = "bolt"
			}

			err = shell.RunSaveCmd(context.Background(), db, engine, saveDB)
			require.NoError(t, err)
			require.NoError(t, db.Close())

			var buf bytes.Buffer
			err = executeDump(context.Background(), &buf, tt.tables, tt.engine, tt.dbPath)
			if tt.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			require.Equal(t, b.String(), buf.String())
		})

	}
}
