package dbutil

import (
	"context"
	"encoding/hex"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji"
)

// OpenDB is a helper function that takes raw unvalidated parameters and opens a database.
func OpenDB(ctx context.Context, dbPath string, encKey string) (*genji.DB, error) {
	if dbPath == "" {
		dbPath = ":memory:"
	}

	// if an encryption key is provided, open the database with the experimental encryption feature.
	// the key must be a 32, 48 or 64 bytes long hexadecimal string.
	var key []byte
	if encKey != "" {
		var err error
		key, err = hex.DecodeString(encKey)
		if err != nil {
			return nil, errors.Wrap(err, "invalid encryption key")
		}
	}

	db, err := genji.OpenWith(dbPath, &genji.Options{
		Experimental: struct{ EncryptionKey []byte }{
			EncryptionKey: key,
		},
	})
	if err != nil {
		return nil, err
	}

	return db.WithContext(ctx), nil
}
