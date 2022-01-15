package database

import (
	"context"
	"testing"

	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/engine/memoryengine"
	"github.com/stretchr/testify/require"
)

func TestTransientStorePool(t *testing.T) {
	db, err := New(context.Background(), memoryengine.NewEngine())
	require.NoError(t, err)

	ctx := context.Background()

	// ask for more than the pool size (3)
	var tempStores []engine.TransientStore
	for i := 0; i < 4; i++ {
		ts, err := db.TransientStorePool.Get(ctx)
		require.NoError(t, err)
		tempStores = append(tempStores, ts)
	}

	require.Nil(t, db.TransientStorePool.pool)

	// release 3 databases and expect the pool to fill up
	for i := 0; i < 3; i++ {
		err = db.TransientStorePool.Release(ctx, tempStores[i])
		require.NoError(t, err)

		require.Len(t, db.TransientStorePool.pool, i+1)
	}

	// if the pool is full, releasing more shouldn't increase the pool
	err = db.TransientStorePool.Release(ctx, tempStores[3])
	require.NoError(t, err)
	require.Len(t, db.TransientStorePool.pool, 3)

	// get should return databases from the pool instead of creating new ones
	for i := 0; i < 3; i++ {
		_, err = db.TransientStorePool.Get(ctx)
		require.NoError(t, err)
		require.Len(t, db.TransientStorePool.pool, 2-i)
	}

	// when the pool is empty, it should create a new engine
	_, err = db.TransientStorePool.Get(ctx)
	require.NoError(t, err)
}
