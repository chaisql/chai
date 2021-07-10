package dbutil

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/genjidb/genji/engine/boltengine"
	"github.com/genjidb/genji/engine/memoryengine"
	"go.etcd.io/bbolt"
)

type DBOptions struct {
	EncryptionKey string
}

// OpenDB opens a database at the given path, using the selected engine.
func OpenDB(ctx context.Context, dbPath, engineName string, opts DBOptions) (*genji.DB, error) {
	var (
		ng  engine.Engine
		err error
	)

	switch engineName {
	case "memory":
		ng = memoryengine.NewEngine()
	case "bolt":
		ng, err = boltengine.NewEngine(dbPath, 0660, &bbolt.Options{
			Timeout: 100 * time.Millisecond,
		})
		if err == bbolt.ErrTimeout {
			return nil, errors.New("database is locked")
		}
	case "badger":
		opt := badger.DefaultOptions(dbPath).WithLogger(nil)
		if opts.EncryptionKey != "" {
			opt.EncryptionKey = []byte(opts.EncryptionKey)
			opt.IndexCacheSize = 100 << 20
		}

		ng, err = badgerengine.NewEngine(opt)
		if err != nil && strings.HasPrefix(err.Error(), "Cannot acquire directory lock") {
			return nil, errors.New("database is locked")
		}
	default:
		return nil, fmt.Errorf(`engine should be "bolt" or "badger", got %q`, engineName)
	}
	if err != nil {
		return nil, err
	}

	return genji.New(ctx, ng)
}
