package shell

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"

	"context"

	"github.com/dgraph-io/badger/v2"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/stretchr/testify/require"
)

func TestRunTablesCmd(t *testing.T) {
	tests := []struct {
		name    string
		in      []string
		wantErr bool
	}{
		{
			"Table",
			strings.Fields(".tables"),
			false,
		},
		{
			"Table with options",
			strings.Fields(".tables test"),
			true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			if err := runTablesCmd(db, test.in); (err != nil) != test.wantErr {
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
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.Open(":memory:")
			require.NoError(t, err)
			defer db.Close()

			err = db.Exec("CREATE TABLE test")
			require.NoError(t, err)
			err = db.Exec(`
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
		{"text / pk and not null with type constraint", `INSERT INTO test (a, b, c) VALUES ('a', 'b', 'c')`, `TEXT PRIMARY KEY NOT NULL DEFAULT "foo"`, `INSERT INTO test VALUES {"a": "a", "b": "b", "c": "c"};`, false, nil},
	}

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
					err := db.Exec(q)
					require.NoError(t, err)
					bwant.WriteString(q)
				} else {
					q := `CREATE TABLE test;`
					err = db.Exec(q)
					require.NoError(t, err)
					q = fmt.Sprintf("%s\n", q)
					bwant.WriteString(q)
				}

				if withIndexes {
					err = db.Exec(`
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
				err = db.Exec(tt.query, tt.params...)
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

func TestSaveCommand(t *testing.T) {
	tests := []struct {
		engine string
		path   string
	}{
		{"bolt", os.TempDir() + "/test.db"},
		{"badger", os.TempDir()},
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
				CREATE TABLE test (a);
				CREATE INDEX idx_a ON test (a);
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
			err = db.View(func(tx *genji.Tx) error {
				indexes, err := tx.ListIndexes()
				require.NoError(t, err)
				require.Len(t, indexes, 1)
				require.Equal(t, "idx_a", indexes[0].IndexName)
				require.Equal(t, "test", indexes[0].TableName)

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
