package database

import (
	"context"
	"sync"

	"github.com/genjidb/genji/engine"
)

const maxTransientPoolSize = 3

// TransientEnginePool manages a pool of transient engines.
// It keeps a pool of maxTransientPoolSize engines.
type TransientEnginePool struct {
	ng engine.Engine

	mu   sync.Mutex
	pool []engine.Engine
}

// Get returns a free engine from the pool, if any. Otherwise it creates a new engine
// and returns it.
func (t *TransientEnginePool) Get(ctx context.Context) (engine.Engine, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.pool) > 0 {
		ng := t.pool[len(t.pool)-1]
		t.pool = t.pool[:len(t.pool)-1]
		return ng, nil
	}

	return t.ng.NewTransientEngine(ctx)
}

// Release sets the engine for reuse. If the pool is full, it drops the given engine.
func (t *TransientEnginePool) Release(ctx context.Context, ng engine.Engine) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.pool) >= maxTransientPoolSize {
		return ng.Drop(ctx)
	}

	t.pool = append(t.pool, ng)
	return nil
}
