package testutil

import (
	"context"
	"testing"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/database/catalogstore"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/kv"
	"github.com/chaisql/chai/internal/query"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/parser"
	"github.com/chaisql/chai/internal/testutil/assert"
	"github.com/chaisql/chai/internal/tree"
	"github.com/stretchr/testify/require"
)

func NewEngine(t testing.TB) *kv.PebbleEngine {
	t.Helper()

	st, err := kv.NewEngine(":memory:", kv.Options{
		RollbackSegmentNamespace: int64(database.RollbackSegmentNamespace),
		MaxBatchSize:             1 << 7,
		MinTransientNamespace:    10_000,
		MaxTransientNamespace:    11_000,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		st.Close()
	})

	return st
}

func NewTestTree(t testing.TB, namespace tree.Namespace) *tree.Tree {
	t.Helper()

	session := NewEngine(t).NewBatchSession()

	t.Cleanup(func() {
		session.Close()
	})

	return tree.New(session, namespace, 0)
}

func NewTestDB(t testing.TB) *database.Database {
	t.Helper()

	db, err := database.Open(":memory:", &database.Options{
		CatalogLoader: catalogstore.LoadCatalog,
	})
	assert.NoError(t, err)

	t.Cleanup(func() {
		db.Close()
	})

	return db
}

func NewTestTx(t testing.TB) (*database.Database, *database.Transaction, func()) {
	t.Helper()

	db := NewTestDB(t)

	tx, err := db.Begin(true)
	assert.NoError(t, err)

	return db, tx, func() {
		tx.Rollback()
	}
}

func Exec(db *database.Database, tx *database.Transaction, q string, params ...environment.Param) error {
	res, err := Query(db, tx, q, params...)
	if err != nil {
		return err
	}
	defer res.Close()

	return res.Iterate(func(database.Row) error {
		return nil
	})
}

func Query(db *database.Database, tx *database.Transaction, q string, params ...environment.Param) (*statement.Result, error) {
	pq, err := parser.ParseQuery(q)
	if err != nil {
		return nil, err
	}

	ctx := &query.Context{Ctx: context.Background(), DB: db, Tx: tx, Params: params}
	err = pq.Prepare(ctx)
	if err != nil {
		return nil, err
	}

	return pq.Run(ctx)
}

func MustExec(t *testing.T, db *database.Database, tx *database.Transaction, q string, params ...environment.Param) {
	t.Helper()

	err := Exec(db, tx, q, params...)
	assert.NoError(t, err)
}

func MustQuery(t *testing.T, db *database.Database, tx *database.Transaction, q string, params ...environment.Param) *statement.Result {
	res, err := Query(db, tx, q, params...)
	assert.NoError(t, err)
	return res
}
