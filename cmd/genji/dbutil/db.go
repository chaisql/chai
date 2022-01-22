package dbutil

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/internal/kv"
)

type DBOptions struct {
	EncryptionKey string
}

// OpenDB opens a database at the given path.
func OpenDB(ctx context.Context, dbPath string, opts DBOptions) (*genji.DB, error) {
	var (
		ng  *kv.Engine
		err error
	)

	opt := badger.DefaultOptions(dbPath).WithLogger(nil)
	if dbPath == "" {
		opt = opt.WithInMemory(true)
	}

	if opts.EncryptionKey != "" {
		opt.EncryptionKey = []byte(opts.EncryptionKey)
		opt.IndexCacheSize = 100 << 20
	}

	ng, err = kv.NewEngine(opt)
	if err != nil && strings.HasPrefix(err.Error(), "Cannot acquire directory lock") {
		return nil, errors.New("database is locked")
	}

	if err != nil {
		return nil, err
	}

	return genji.New(ctx, ng)
}
