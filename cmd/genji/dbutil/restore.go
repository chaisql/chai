package dbutil

import (
	"context"
	"io/ioutil"
	"os"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji"
)

// Restore a database from a file created by genji dump.
// This function can be provided with an existing database (genji cli use case),
// otherwise new database is being created.
func Restore(ctx context.Context, db *genji.DB, dumpFile, dbPath string) error {
	if dbPath == "" {
		return errors.New("database path expected")
	}

	if dumpFile == "" {
		return errors.New("dump file expected")
	}

	file, err := os.Open(dumpFile)
	if err != nil {
		return err
	}
	defer file.Close()

	if db == nil {
		db, err = OpenDB(ctx, dbPath)
		if err != nil {
			return err
		}
		defer db.Close()
	}

	return ExecSQL(ctx, db, file, ioutil.Discard)
}
