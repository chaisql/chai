package testutil

import (
	"context"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding/msgpack"
	"github.com/genjidb/genji/engine/memoryengine"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/query"
	"github.com/genjidb/genji/sql/parser"
	"github.com/stretchr/testify/require"
)

func NewTestDB(t testing.TB) (*database.Database, func()) {
	t.Helper()

	db, err := database.New(context.Background(), memoryengine.NewEngine(), database.Options{
		Codec: msgpack.NewCodec(),
	})
	require.NoError(t, err)

	db.Catalog.Load(nil, nil)

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

func Exec(tx *database.Transaction, q string, params ...expr.Param) error {
	res, err := Query(tx, q, params...)
	if err != nil {
		return err
	}

	return res.Iterate(func(d document.Document) error {
		return nil
	})
}

func Query(tx *database.Transaction, q string, params ...expr.Param) (*query.Result, error) {
	pq, err := parser.ParseQuery(q)
	if err != nil {
		return nil, err
	}

	return pq.Exec(tx, params)
}

func MustExec(t *testing.T, tx *database.Transaction, q string, params ...expr.Param) {
	err := Exec(tx, q, params...)
	require.NoError(t, err)
}

func MustQuery(t *testing.T, tx *database.Transaction, q string, params ...expr.Param) *query.Result {
	res, err := Query(tx, q, params...)
	require.NoError(t, err)
	return res
}
