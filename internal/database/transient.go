package database

import (
	"context"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/genjidb/genji/internal/kv"
	"github.com/genjidb/genji/internal/tree"
)

const maxTransientPoolSize = 3

// TransientStorePool manages a pool of transient stores.
// It keeps a pool of maxTransientPoolSize stores.
type TransientStorePool struct {
	pdb  *pebble.DB
	opts *pebble.Options

	mu   sync.Mutex
	Pool []*kv.TransientStore
}

// Get returns a free engine from the pool, if any. Otherwise it creates a new engine
// and returns it.
func (t *TransientStorePool) Get(ctx context.Context) (*kv.TransientStore, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.Pool) > 0 {
		ng := t.Pool[len(t.Pool)-1]
		err := ng.Reset()
		if err != nil {
			return nil, err
		}

		t.Pool = t.Pool[:len(t.Pool)-1]
		return ng, nil
	}

	return kv.NewTransientStore(t.opts)
}

// Release sets the store for reuse. If the pool is full, it drops the given store.
func (t *TransientStorePool) Release(ctx context.Context, ts *kv.TransientStore) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.Pool) >= maxTransientPoolSize {
		return ts.Drop()
	}

	t.Pool = append(t.Pool, ts)
	return nil
}

// NewTransientTree creates a temporary tree.
func NewTransientTree(db *Database) (*tree.Tree, func() error, error) {
	ts, cleanup, err := db.NewTransientStore(context.Background())
	if err != nil {
		return nil, nil, err
	}

	return tree.NewTransient(ts), cleanup, nil
}
