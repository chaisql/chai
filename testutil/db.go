package testutil

import (
	"context"
	"testing"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document/encoding/msgpack"
	"github.com/genjidb/genji/engine/memoryengine"
	"github.com/stretchr/testify/require"
)

func NewTestDB(t testing.TB) (*database.Database, func()) {
	t.Helper()

	db, err := database.New(context.Background(), memoryengine.NewEngine(), database.Options{
		Codec: msgpack.NewCodec(),
	})
	require.NoError(t, err)

	return db, func() {
		db.Close()
	}
}

func NewTestTx(t testing.TB) (*database.Database, *database.Transaction, func()) {
	t.Helper()

	db, cleanup := NewTestDB(t)

	tx, err := db.Begin(true)
	require.NoError(t, err)

	return db, tx, func() {
		tx.Rollback()
		cleanup()
	}
}
