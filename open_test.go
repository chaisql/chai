package genji_test

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	dir, err := ioutil.TempDir("", "genji")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := genji.Open(filepath.Join(dir, "test.db"))
	require.NoError(t, err)

	err = db.Exec(`
		CREATE TABLE tableA (a INTEGER UNIQUE NOT NULL, b.c[0].d DOUBLE PRIMARY KEY);
		CREATE TABLE tableB (a TEXT NOT NULL DEFAULT 'hello', PRIMARY KEY (a));
		CREATE TABLE tableC;
		CREATE INDEX tableC_a_b_idx ON tableC(a, b);
		CREATE SEQUENCE seqD INCREMENT BY 10 CYCLE MINVALUE 100 NO MAXVALUE START 500;

		INSERT INTO tableB (a) VALUES (1);
		INSERT INTO tableC (a, b) VALUES (1, NEXT VALUE FOR seqD);
	`)
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)

	// ensure tables are loaded properly
	db, err = genji.Open(filepath.Join(dir, "test.db"))
	require.NoError(t, err)
	defer db.Close()

	res1, err := db.Query("SELECT * FROM __genji_catalog")
	require.NoError(t, err)
	defer res1.Close()

	var count int
	err = res1.Iterate(func(d document.Document) error {
		count++
		if count == 1 {
			testutil.RequireDocJSONEq(t, d, `{"name":"seqD", "sql":"CREATE SEQUENCE seqD INCREMENT BY 10 MINVALUE 100 START WITH 500 CYCLE", "type":"sequence"}`)
			return nil
		}

		if count == 2 {
			testutil.RequireDocJSONEq(t, d, `{"name":"tableA", "sql":"CREATE TABLE tableA (a INTEGER NOT NULL UNIQUE, b.c[0].d DOUBLE PRIMARY KEY)", "store_name":"AQ==", "type":"table"}`)
			return nil
		}

		if count == 3 {
			testutil.RequireDocJSONEq(t, d, `{"constraint_path":"a", "name":"tableA_a_idx", "sql":"CREATE UNIQUE INDEX tableA_a_idx ON tableA (a)", "store_name":"Ag==", "table_name":"tableA", "type":"index"}`)
			return nil
		}

		if count == 4 {
			testutil.RequireDocJSONEq(t, d, `{"name":"tableB", "sql":"CREATE TABLE tableB (a TEXT NOT NULL PRIMARY KEY DEFAULT \"hello\")", "store_name":"Aw==", "type":"table"}`)
			return nil
		}

		if count == 5 {
			testutil.RequireDocJSONEq(t, d, `{"name":"tableC", "sql":"CREATE TABLE tableC", "store_name":"BA==", "type":"table"}`)
			return nil
		}

		if count == 6 {
			testutil.RequireDocJSONEq(t, d, `{"name":"tableC_a_b_idx", "sql":"CREATE INDEX tableC_a_b_idx ON tableC (a, b)", "store_name":"BQ==", "table_name":"tableC", "type":"index"}`)
			return nil
		}

		return errors.New("more than 6 relations")
	})
	require.NoError(t, err)

	d, err := db.QueryDocument("SELECT * FROM tableB")
	require.NoError(t, err)
	testutil.RequireDocJSONEq(t, d, `{"a": "1"}`)

	d, err = db.QueryDocument("SELECT * FROM __genji_sequence")
	require.NoError(t, err)
	testutil.RequireDocJSONEq(t, d, `{"name": "seqD", "seq": 500}`)
}
