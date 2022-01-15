package database_test

import (
	"context"
	"testing"

	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
)

func newTestDB(t testing.TB) (*database.Database, func()) {
	t.Helper()

	db, err := database.New(context.Background(), testutil.NewEngine(t))
	assert.NoError(t, err)

	return db, func() {
		db.Close()
	}
}

func newTestTx(t testing.TB) (*database.Database, *database.Transaction, func()) {
	t.Helper()

	db, cleanup := newTestDB(t)

	tx, err := db.Begin(true)
	assert.NoError(t, err)

	return db, tx, func() {
		tx.Rollback()
		cleanup()
	}
}
