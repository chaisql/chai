package dbutil

import (
	"context"

	"github.com/genjidb/genji"
)

// OpenDB is a helper function that takes raw unvalidated parameters and opens a database.
func OpenDB(ctx context.Context, dbPath string) (*genji.DB, error) {
	if dbPath == "" {
		dbPath = ":memory:"
	}

	db, err := genji.Open(dbPath)
	if err != nil {
		return nil, err
	}

	return db.WithContext(ctx), nil
}
