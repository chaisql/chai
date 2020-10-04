package shell

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/genjidb/genji"
	"github.com/stretchr/testify/require"
)

func TestRunTablesCmd(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr bool
	}{
		{
			"Table",
			".tables",
			false,
		},
		{
			"Table with options",
			".tables test",
			true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			if err := displayTableIndex(db, test.in); (err != nil) != test.wantErr {
				require.Errorf(t, err, "", test.wantErr)
			}
		})
	}
}

func TestIndexesCmd(t *testing.T) {
	tests := []struct {
		name    string
		in      []string
		wantErr bool
	}{
		{
			".Indexes",
			strings.Fields(".indexes"),
			false,
		},
		{
			"Indexes with table name",
			strings.Fields(".indexes test"),
			false,
		},
		{
			"Indexes with nonexistent table name",
			strings.Fields(".indexes foo"),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			ctx := context.Background()

			err = db.Exec(ctx, "CREATE TABLE test")
			require.NoError(t, err)
			err = db.Exec(ctx, `
						CREATE INDEX idx_a ON test (a);
						CREATE INDEX idx_b ON test (b);
						CREATE INDEX idx_c ON test (c);
					`)
			require.NoError(t, err)
			if err := runIndexesCmd(db, test.in); (err != nil) != test.wantErr {
				require.Errorf(t, err, "", test.wantErr)
			}
		})
	}
}

func TestRunDumpCmd(t *testing.T) {
	tests := []struct {
		name            string
		query           string
		fieldConstraint string
		want            string
		fails           bool
		params          []interface{}
	}{
		{"Values / With columns", `INSERT INTO test (a, b, c) VALUES ('a', 'b', 'c')`, ``, `INSERT INTO test VALUES {"a": "a", "b": "b", "c": "c"};`, false, nil},
		{"text / not null with type constraint", `INSERT INTO test (a, b, c) VALUES ('a', 'b', 'c')`, `TEXT NOT NULL`, `INSERT INTO test VALUES {"a": "a", "b": "b", "c": "c"};`, false, nil},
		{"text / pk and not null with type constraint", `INSERT INTO test (a, b, c) VALUES ('a', 'b', 'c')`, `TEXT PRIMARY KEY NOT NULL`, `INSERT INTO test VALUES {"a": "a", "b": "b", "c": "c"};`, false, nil},
	}

	ctx := context.Background()

	for _, tt := range tests {

		testFn := func(withIndexes, withConstraints bool) func(t *testing.T) {
			return func(t *testing.T) {
				db, err := genji.Open(":memory:")
				require.NoError(t, err)
				defer db.Close()

				var bwant bytes.Buffer

				tx := "BEGIN TRANSACTION;\n"
				bwant.WriteString(tx)
				ci := "COMMIT;\n"
				if withConstraints {
					q := fmt.Sprintf("CREATE TABLE test (\n  a %s\n);\n", tt.fieldConstraint)
					err := db.Exec(ctx, q)
					require.NoError(t, err)
					bwant.WriteString(q)
				} else {
					q := `CREATE TABLE test;`
					err = db.Exec(ctx, q)
					require.NoError(t, err)
					q = fmt.Sprintf("%s\n", q)
					bwant.WriteString(q)
				}

				if withIndexes {
					err = db.Exec(ctx, `
						CREATE INDEX idx_a ON test (a);
					`)
					require.NoError(t, err)
					err = db.View(func(tx *genji.Tx) error {
						// indexes is unordered, we cannot guess the order.
						// we have to test only one index creation.
						indexes, err := tx.ListIndexes()
						require.NoError(t, err)
						for _, index := range indexes {
							info := fmt.Sprintf("CREATE INDEX %s ON %s (%s);\n", index.IndexName, index.TableName,
								index.Path)
							bwant.WriteString(info)
						}
						return nil
					})
					require.NoError(t, err)

				}
				err = db.Exec(context.Background(), tt.query, tt.params...)
				if tt.fails {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				tt.want = fmt.Sprintf("%s\n", strings.TrimSpace(tt.want))
				bwant.WriteString(tt.want)

				var buf bytes.Buffer
				err = runDumpCmd(db, []string{`test`}, &buf)
				require.NoError(t, err)
				bwant.WriteString(ci)
				require.Equal(t, bwant.String(), buf.String())

			}
		}

		t.Run("No Index/"+tt.name, testFn(false, false))
		t.Run("With Index/"+tt.name, testFn(true, false))
		t.Run("With FieldsConstraints/"+tt.name, testFn(true, true))
	}

}
