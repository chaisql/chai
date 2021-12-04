package testutil

import (
	"context"
	"testing"

	"github.com/genjidb/genji/document/encoding/msgpack"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/engine/memoryengine"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/database/catalogstore"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/query"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func NewTestStore(t testing.TB, name string) engine.Store {
	t.Helper()

	ng := memoryengine.NewEngine()

	tx, err := ng.Begin(context.Background(), engine.TxOptions{Writable: true})
	require.NoError(t, err)

	err = tx.CreateStore([]byte(name))
	require.NoError(t, err)

	st, err := tx.GetStore([]byte(name))
	require.NoError(t, err)

	t.Cleanup(func() {
		tx.Rollback()
		ng.Close()
	})

	return st
}

func NewTestDB(t testing.TB) (*database.Database, func()) {
	t.Helper()

	return NewTestDBWithEngine(t, memoryengine.NewEngine())
}

func NewTestDBWithEngine(t testing.TB, ng engine.Engine) (*database.Database, func()) {
	t.Helper()

	db, err := database.New(context.Background(), ng, database.Options{
		Codec: msgpack.NewCodec(),
	})
	assert.NoError(t, err)

	LoadCatalog(t, db)

	return db, func() {
		db.Close()
	}
}

func LoadCatalog(t testing.TB, db *database.Database) {
	tx, err := db.Begin(true)
	assert.NoError(t, err)
	defer tx.Rollback()

	err = catalogstore.LoadCatalog(tx, db.Catalog)
	assert.NoError(t, err)

	err = tx.Commit()
	assert.NoError(t, err)
}

func NewTestTx(t testing.TB) (*database.Database, *database.Transaction, func()) {
	t.Helper()

	db, cleanup := NewTestDB(t)

	tx, err := db.Begin(true)
	assert.NoError(t, err)

	return db, tx, func() {
		tx.Rollback()
		cleanup()
	}
}

func Exec(db *database.Database, tx *database.Transaction, q string, params ...environment.Param) error {
	res, err := Query(db, tx, q, params...)
	if err != nil {
		return err
	}
	defer res.Close()

	return res.Iterate(func(d types.Document) error {
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
	err := Exec(db, tx, q, params...)
	assert.NoError(t, err)
}

func MustQuery(t *testing.T, db *database.Database, tx *database.Transaction, q string, params ...environment.Param) *statement.Result {
	res, err := Query(db, tx, q, params...)
	assert.NoError(t, err)
	return res
}
