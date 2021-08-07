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
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func NewTestDB(t testing.TB) (*database.Database, func()) {
	t.Helper()

	return NewTestDBWithEngine(t, memoryengine.NewEngine())
}

func NewTestDBWithEngine(t testing.TB, ng engine.Engine) (*database.Database, func()) {
	t.Helper()

	db, err := database.New(context.Background(), ng, database.Options{
		Codec: msgpack.NewCodec(),
	})
	require.NoError(t, err)

	LoadCatalog(t, db)

	return db, func() {
		db.Close()
	}
}

func LoadCatalog(t testing.TB, db *database.Database) {
	tx, err := db.Begin(true)
	require.NoError(t, err)
	defer tx.Rollback()

	err = catalogstore.LoadCatalog(tx, db.Catalog)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)
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

func Exec(db *database.Database, tx *database.Transaction, q string, params ...environment.Param) error {
	res, err := Query(db, tx, q, params...)
	if err != nil {
		return err
	}

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
	require.NoError(t, err)
}

func MustQuery(t *testing.T, db *database.Database, tx *database.Transaction, q string, params ...environment.Param) *statement.Result {
	res, err := Query(db, tx, q, params...)
	require.NoError(t, err)
	return res
}
