package dbutil

import (
	"context"

	"github.com/genjidb/genji"
)

// OpenDB opens a database at the given path.
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
