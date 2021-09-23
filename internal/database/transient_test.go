package database

import (
	"context"
	"testing"

	"github.com/genjidb/genji/document/encoding/msgpack"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/engine/memoryengine"
	"github.com/stretchr/testify/require"
)

func TestTransientEnginePool(t *testing.T) {
	db, err := New(context.Background(), memoryengine.NewEngine(), Options{Codec: msgpack.NewCodec()})
	require.NoError(t, err)

	ctx := context.Background()

	// ask for more than the pool size (3)
	var engines []engine.Engine
	for i := 0; i < 4; i++ {
		ng, err := db.TransientEnginePool.Get(ctx)
		require.NoError(t, err)
		engines = append(engines, ng)
	}

	require.Nil(t, db.TransientEnginePool.pool)

	// release 3 engines and expect the pool to fill up
	for i := 0; i < 3; i++ {
		err = db.TransientEnginePool.Release(ctx, engines[i])
		require.NoError(t, err)

		require.Len(t, db.TransientEnginePool.pool, i+1)
	}

	// if the pool is full, releasing more shouldn't increase the pool
	err = db.TransientEnginePool.Release(ctx, engines[3])
	require.NoError(t, err)
	require.Len(t, db.TransientEnginePool.pool, 3)

	// get should return engines from the pool instead of creating new ones
	for i := 0; i < 3; i++ {
		_, err = db.TransientEnginePool.Get(ctx)
		require.NoError(t, err)
		require.Len(t, db.TransientEnginePool.pool, 2-i)
	}

	// when the pool is empty, it should create a new engine
	_, err = db.TransientEnginePool.Get(ctx)
	require.NoError(t, err)
}
