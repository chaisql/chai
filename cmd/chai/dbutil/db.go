package dbutil

import (
	"context"

	"github.com/chaisql/chai"
)

// OpenDB is a helper function that takes raw unvalidated parameters and opens a database.
func OpenDB(ctx context.Context, dbPath string) (*chai.DB, error) {
	if dbPath == "" {
		dbPath = ":memory:"
	}

	db, err := chai.Open(dbPath)
	if err != nil {
		return nil, err
	}

	return db.WithContext(ctx), nil
}
