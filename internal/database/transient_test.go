package database

import (
	"context"
	"testing"

	"github.com/genjidb/genji/engine/memoryengine"
	"github.com/stretchr/testify/require"
)

func TestTransientDatabasePool(t *testing.T) {
	db, err := New(context.Background(), memoryengine.NewEngine())
	require.NoError(t, err)

	ctx := context.Background()

	// ask for more than the pool size (3)
	var tempDBs []*Database
	for i := 0; i < 4; i++ {
		tdb, err := db.TransientDatabasePool.Get(ctx)
		require.NoError(t, err)
		tempDBs = append(tempDBs, tdb)
	}

	require.Nil(t, db.TransientDatabasePool.pool)

	// release 3 databases and expect the pool to fill up
	for i := 0; i < 3; i++ {
		err = db.TransientDatabasePool.Release(ctx, tempDBs[i])
		require.NoError(t, err)

		require.Len(t, db.TransientDatabasePool.pool, i+1)
	}

	// if the pool is full, releasing more shouldn't increase the pool
	err = db.TransientDatabasePool.Release(ctx, tempDBs[3])
	require.NoError(t, err)
	require.Len(t, db.TransientDatabasePool.pool, 3)

	// get should return databases from the pool instead of creating new ones
	for i := 0; i < 3; i++ {
		_, err = db.TransientDatabasePool.Get(ctx)
		require.NoError(t, err)
		require.Len(t, db.TransientDatabasePool.pool, 2-i)
	}

	// when the pool is empty, it should create a new engine
	_, err = db.TransientDatabasePool.Get(ctx)
	require.NoError(t, err)
}
