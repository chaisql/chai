package database_test

import (
	"context"
	"testing"

	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/kv"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestTransientStorePool(t *testing.T) {
	db, err := database.New(context.Background(), testutil.NewEngine(t))
	require.NoError(t, err)

	ctx := context.Background()

	// ask for more than the pool size (3)
	var tempStores []*kv.TransientStore
	for i := 0; i < 4; i++ {
		ts, err := db.TransientStorePool.Get(ctx)
		require.NoError(t, err)
		tempStores = append(tempStores, ts)
	}

	require.Nil(t, db.TransientStorePool.Pool)

	// release 3 databases and expect the pool to fill up
	for i := 0; i < 3; i++ {
		err = db.TransientStorePool.Release(ctx, tempStores[i])
		require.NoError(t, err)

		require.Len(t, db.TransientStorePool.Pool, i+1)
	}

	// if the pool is full, releasing more shouldn't increase the pool
	err = db.TransientStorePool.Release(ctx, tempStores[3])
	require.NoError(t, err)
	require.Len(t, db.TransientStorePool.Pool, 3)

	// get should return databases from the pool instead of creating new ones
	for i := 0; i < 3; i++ {
		_, err = db.TransientStorePool.Get(ctx)
		require.NoError(t, err)
		require.Len(t, db.TransientStorePool.Pool, 2-i)
	}

	// when the pool is empty, it should create a new engine
	_, err = db.TransientStorePool.Get(ctx)
	require.NoError(t, err)
}
