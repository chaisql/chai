package dbutil

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/genjidb/genji"
	"github.com/stretchr/testify/require"
)

func TestDump(t *testing.T) {
	tests := []struct {
		name   string
		tables []string
	}{
		{"All tables", nil},
		{"Selection of tables", []string{"test", "foo"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			var want bytes.Buffer
			want.WriteString("BEGIN TRANSACTION;\n")

			getBuffer := func(table string) func(s string) {
				writeFn := func(s string) {
					want.WriteString(s)
				}
				noOp := func(s string) {}

				if len(tt.tables) == 0 {
					return writeFn
				}

				for _, tb := range tt.tables {
					if tb == table {
						return writeFn
					}
				}
				return noOp
			}

			for i, table := range []string{"tblA", "tblB"} {
				writeToBuf := getBuffer(table)

				if i > 0 {
					writeToBuf("\n")
				}

				q := fmt.Sprintf("CREATE TABLE %s (a INTEGER);", table)
				err = db.Exec(q)
				require.NoError(t, err)
				writeToBuf(q + "\n")

				q = fmt.Sprintf(`CREATE INDEX idx_%s_a ON %s (a);`, table, table)
				err = db.Exec(q)
				require.NoError(t, err)
				writeToBuf(q + "\n")

				q = fmt.Sprintf(`CREATE INDEX idx_%s_b_c ON %s (b, c);`, table, table)
				err = db.Exec(q)
				require.NoError(t, err)
				writeToBuf(q + "\n")

				q = fmt.Sprintf(`INSERT INTO %s VALUES {"a": %d, "b": %d, "c": %d};`, table, 1, 2, 3)
				err = db.Exec(q)
				require.NoError(t, err)
				writeToBuf(q + "\n")

				q = fmt.Sprintf(`INSERT INTO %s VALUES {"a": %d, "b": %d, "c": %d};`, table, 2, 2, 2)
				err = db.Exec(q)
				require.NoError(t, err)
				writeToBuf(q + "\n")

				q = fmt.Sprintf(`INSERT INTO %s VALUES {"a": %d, "b": %d, "c": %d};`, table, 3, 2, 1)
				err = db.Exec(q)
				require.NoError(t, err)
				writeToBuf(q + "\n")
			}
			want.WriteString("COMMIT;\n")

			var got bytes.Buffer
			err = Dump(context.Background(), db, &got, tt.tables...)
			require.NoError(t, err)

			require.Equal(t, want.String(), got.String())
		})
	}
}

func TestDumpSchema(t *testing.T) {
	tests := []struct {
		name   string
		tables []string
	}{
		{"All tables", nil},
		{"Selection of tables", []string{"test", "foo"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			var want bytes.Buffer

			getBuffer := func(table string) func(s string) {
				writeFn := func(s string) {
					want.WriteString(s)
				}
				noOp := func(s string) {}

				if len(tt.tables) == 0 {
					return writeFn
				}

				for _, tb := range tt.tables {
					if tb == table {
						return writeFn
					}
				}
				return noOp
			}

			for i, table := range []string{"tblA", "tblB"} {
				writeToBuf := getBuffer(table)

				if i > 0 {
					writeToBuf("\n")
				}

				q := fmt.Sprintf("CREATE TABLE %s (a INTEGER);", table)
				err = db.Exec(q)
				require.NoError(t, err)
				writeToBuf(q + "\n")

				q = fmt.Sprintf(`CREATE INDEX idx_a_%s ON %s (a);`, table, table)
				err = db.Exec(q)
				require.NoError(t, err)
				writeToBuf(q + "\n")
			}

			var got bytes.Buffer
			err = DumpSchema(context.Background(), db, &got, tt.tables...)
			require.NoError(t, err)

			require.Equal(t, want.String(), got.String())
		})
	}
}
