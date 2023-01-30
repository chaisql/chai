package database_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestEncryptionKeySize(t *testing.T) {
	tests := []struct {
		name  string
		key   []byte
		fails bool
	}{
		// no key
		{name: "no-key", key: nil, fails: false},
		// empty key
		{name: "empty-key", key: []byte{}, fails: true},
		// 16 bytes
		{name: "16-bytes", key: []byte("1234567890123456"), fails: false},
		// 24 bytes
		{name: "24-bytes", key: []byte("123456789012345678901234"), fails: false},
		// 32 bytes
		{name: "32-bytes", key: []byte("12345678901234567890123456789012"), fails: false},
		// invalid number of bytes
		{name: "invalid", key: []byte("12345678901234567890123456789012345678901234567890123456789012345"), fails: true},
	}

	dir, err := ioutil.TempDir("", "genji")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := genji.OpenWith(filepath.Join(dir, test.name), &genji.Options{
				Experimental: struct{ EncryptionKey []byte }{
					EncryptionKey: test.key,
				},
			})
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				err = db.Close()
				require.NoError(t, err)
			}
		})
	}
}

func TestEncryption(t *testing.T) {
	dir, err := ioutil.TempDir("", "genji")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	openDB := func() *genji.DB {
		db, err := genji.OpenWith(filepath.Join(dir, "encdb"), &genji.Options{
			Experimental: struct{ EncryptionKey []byte }{
				EncryptionKey: []byte("12345678901234567890123456789012"),
			},
		})
		require.NoError(t, err)
		return db
	}

	read := func(db *genji.DB) {
		res, err := db.Query("SELECT * FROM foo")
		require.NoError(t, err)
		defer res.Close()

		err = res.Iterate(func(d types.Document) error {
			var a int
			var b string

			err = document.Scan(d, &a, &b)
			require.NoError(t, err)
			require.Equal(t, a, 1)
			require.Equal(t, b, "foo")

			return nil
		})
		require.NoError(t, err)
	}

	// create an encrypted database
	// and insert some data
	db := openDB()
	err = db.Exec("CREATE TABLE foo(a int, b text unique)")
	require.NoError(t, err)
	err = db.Exec("INSERT INTO foo VALUES (1, 'foo')")
	require.NoError(t, err)

	// read the data
	read(db)

	// close the db
	require.NoError(t, db.Close())

	fmt.Println("Close db and reopen it")
	// open the database again
	// and read the data
	db = openDB()
	read(db)
	require.NoError(t, db.Close())
}
