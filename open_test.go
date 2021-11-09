package genji_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
)

func TestOpen(t *testing.T) {
	dir, err := ioutil.TempDir("", "genji")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := genji.Open(filepath.Join(dir, "test.db"))
	assert.NoError(t, err)

	err = db.Exec(`
		CREATE TABLE tableA (a INTEGER UNIQUE NOT NULL, b.c[0].d DOUBLE PRIMARY KEY);
		CREATE TABLE tableB (a TEXT NOT NULL DEFAULT 'hello', PRIMARY KEY (a));
		CREATE TABLE tableC;
		CREATE INDEX tableC_a_b_idx ON tableC(a, b);
		CREATE SEQUENCE seqD INCREMENT BY 10 CYCLE MINVALUE 100 NO MAXVALUE START 500;

		INSERT INTO tableB (a) VALUES (1);
		INSERT INTO tableC (a, b) VALUES (1, NEXT VALUE FOR seqD);
	`)
	assert.NoError(t, err)

	err = db.Close()
	assert.NoError(t, err)

	// ensure tables are loaded properly
	db, err = genji.Open(filepath.Join(dir, "test.db"))
	assert.NoError(t, err)
	defer db.Close()

	res1, err := db.Query("SELECT * FROM __genji_catalog")
	assert.NoError(t, err)
	defer res1.Close()

	var count int
	err = res1.Iterate(func(d types.Document) error {
		count++
		if count == 1 {
			testutil.RequireDocJSONEq(t, d, `{"name":"__genji_sequence", "sql":"CREATE TABLE __genji_sequence (name TEXT, seq INTEGER, PRIMARY KEY (name))", "store_name":"X19nZW5qaV9zZXF1ZW5jZQ==", "type":"table"}`)
			return nil
		}

		if count == 2 {
			testutil.RequireDocJSONEq(t, d, `{"name":"__genji_store_seq", "owner":{"table_name":"__genji_catalog"}, "sql":"CREATE SEQUENCE __genji_store_seq CACHE 16", "type":"sequence"}`)
			return nil
		}

		if count == 3 {
			testutil.RequireDocJSONEq(t, d, `{"name":"seqD", "sql":"CREATE SEQUENCE seqD INCREMENT BY 10 MINVALUE 100 START WITH 500 CYCLE", "type":"sequence"}`)
			return nil
		}

		if count == 4 {
			testutil.RequireDocJSONEq(t, d, `{"name":"tableA", "sql":"CREATE TABLE tableA (a INTEGER NOT NULL, b.c[0].d DOUBLE, UNIQUE (a), PRIMARY KEY (b.c[0].d))", "store_name":"AQ==", "type":"table"}`)
			return nil
		}

		if count == 5 {
			testutil.RequireDocJSONEq(t, d, `{"name":"tableA_a_idx", "owner":{"table_name":"tableA", "path":"a"}, "sql":"CREATE UNIQUE INDEX tableA_a_idx ON tableA (a)", "store_name":"Ag==", "table_name":"tableA", "type":"index"}`)
			return nil
		}

		if count == 6 {
			testutil.RequireDocJSONEq(t, d, `{"name":"tableB", "sql":"CREATE TABLE tableB (a TEXT NOT NULL DEFAULT \"hello\", PRIMARY KEY (a))", "store_name":"Aw==", "type":"table"}`)
			return nil
		}

		if count == 7 {
			testutil.RequireDocJSONEq(t, d, `{"name":"tableC", "docid_sequence_name":"tableC_seq", "sql":"CREATE TABLE tableC", "store_name":"BA==", "type":"table"}`)
			return nil
		}

		if count == 8 {
			testutil.RequireDocJSONEq(t, d, `{"name":"tableC_a_b_idx", "sql":"CREATE INDEX tableC_a_b_idx ON tableC (a, b)", "store_name":"BQ==", "table_name":"tableC", "type":"index"}`)
			return nil
		}

		if count == 9 {
			testutil.RequireDocJSONEq(t, d, `{"name":"tableC_seq", "owner":{"table_name":"tableC"}, "sql":"CREATE SEQUENCE tableC_seq CACHE 64", "type":"sequence"}`)
			return nil
		}

		return errors.New("more than 8 relations")
	})
	assert.NoError(t, err)

	d, err := db.QueryDocument("SELECT * FROM tableB")
	assert.NoError(t, err)
	testutil.RequireDocJSONEq(t, d, `{"a": "1"}`)

	d, err = db.QueryDocument("SELECT * FROM __genji_sequence")
	assert.NoError(t, err)
	testutil.RequireDocJSONEq(t, d, `{"name":"__genji_store_seq", "seq":5}`)

	d, err = db.QueryDocument("SELECT * FROM __genji_sequence OFFSET 1")
	assert.NoError(t, err)
	testutil.RequireDocJSONEq(t, d, `{"name": "seqD", "seq": 500}`)
}
