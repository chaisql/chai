package database

import (
	"context"
	"sync"

	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/internal/tree"
)

const maxTransientPoolSize = 3

// TransientStorePool manages a pool of transient stores.
// It keeps a pool of maxTransientPoolSize stores.
type TransientStorePool struct {
	ng engine.Engine

	mu   sync.Mutex
	pool []engine.TransientStore
}

// Get returns a free engine from the pool, if any. Otherwise it creates a new engine
// and returns it.
func (t *TransientStorePool) Get(ctx context.Context) (engine.TransientStore, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.pool) > 0 {
		ng := t.pool[len(t.pool)-1]
		err := ng.Reset()
		if err != nil {
			return nil, err
		}

		t.pool = t.pool[:len(t.pool)-1]
		return ng, nil
	}

	return t.ng.NewTransientStore(ctx)
}

// Release sets the store for reuse. If the pool is full, it drops the given store.
func (t *TransientStorePool) Release(ctx context.Context, ts engine.TransientStore) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.pool) >= maxTransientPoolSize {
		return ts.Drop(ctx)
	}

	t.pool = append(t.pool, ts)
	return nil
}

// NewTransientTree creates a temporary tree.
func NewTransientTree(db *Database) (*tree.Tree, func() error, error) {
	ts, cleanup, err := db.NewTransientStore(context.Background())
	if err != nil {
		return nil, nil, err
	}

	return tree.New(ts), cleanup, nil
}
