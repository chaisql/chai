package dbutil

import (
	"context"
	"io"
	"os"

	"github.com/chaisql/chai"
	"github.com/cockroachdb/errors"
)

// Restore a database from a file created by chai dump.
// This function can be provided with an existing database (chai cli use case),
// otherwise new database is being created.
func Restore(ctx context.Context, db *chai.DB, dumpFile, dbPath string) error {
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

	return ExecSQL(ctx, db, file, io.Discard)
}
