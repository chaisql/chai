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
		CREATE TABLE tableA (a INTEGER NOT NULL, b.c[0].d DOUBLE UNIQUE PRIMARY KEY);
		CREATE TABLE tableB (a TEXT NOT NULL DEFAULT 'hello', PRIMARY KEY (a));
		CREATE TABLE tableC;

		INSERT INTO tableB (a) VALUES (1)
	`)
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)

	// ensure tables are loaded properly
	db, err = genji.Open(filepath.Join(dir, "test.db"))
	require.NoError(t, err)
	defer db.Close()

	res1, err := db.Query("SELECT * FROM __genji_tables")
	require.NoError(t, err)
	defer res1.Close()

	var count int
	err = res1.Iterate(func(d document.Document) error {
		count++
		if count == 1 {
			fb := testutil.MakeDocument(t, `{
				"sql": "CREATE TABLE tableA (a INTEGER NOT NULL, b.c[0].d DOUBLE PRIMARY KEY)",
				"table_name": "tableA"
			}`).(*document.FieldBuffer)

			fb.Add("store_name", document.NewBlobValue([]byte{116, 1}))
			testutil.RequireDocEqual(t, fb, d)
			return nil
		}

		if count == 2 {
			fb := testutil.MakeDocument(t, `{
				"sql": "CREATE TABLE tableB (a TEXT NOT NULL DEFAULT \"hello\" PRIMARY KEY)",
				"table_name": "tableB"
			}`).(*document.FieldBuffer)

			fb.Add("store_name", document.NewBlobValue([]byte{116, 2}))
			testutil.RequireDocEqual(t, fb, d)
			return nil
		}

		if count == 3 {
			fb := testutil.MakeDocument(t, `{
				"sql": "CREATE TABLE tableC",
				"table_name": "tableC"
			}`).(*document.FieldBuffer)

			fb.Add("store_name", document.NewBlobValue([]byte{116, 3}))
			testutil.RequireDocEqual(t, fb, d)
			return nil
		}
		return errors.New("more than 3 tables")
	})
	require.NoError(t, err)

	res2, err := db.Query("SELECT * FROM __genji_indexes")
	require.NoError(t, err)
	defer res2.Close()

	count = 0
	err = res2.Iterate(func(d document.Document) error {
		count++
		if count == 1 {
			fb := testutil.MakeDocument(t, `{
				"sql": "CREATE INDEX __genji_autoindex_tableA_1 ON tableA (b.c[0].d)",
				"index_name": "__genji_autoindex_tableA_1",
				"table_name": "tableA"
			}`).(*document.FieldBuffer)

			testutil.RequireDocEqual(t, fb, d)
			return nil
		}

		return errors.New("more than 1 index")
	})
	require.NoError(t, err)

	d, err := db.QueryDocument("SELECT * FROM tableB")
	require.NoError(t, err)

	testutil.RequireDocJSONEq(t, d, `{"a": "1"}`)
}
