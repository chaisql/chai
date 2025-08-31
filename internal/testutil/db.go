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
	require.NoError(t, err)

	t.Cleanup(func() {
		db.Close()
	})

	return db
}

func NewTestConn(t testing.TB, db *database.Database) *database.Connection {
	t.Helper()

	conn, err := db.Connect()
	require.NoError(t, err)

	t.Cleanup(func() {
		conn.Close()
	})

	return conn
}

func NewTestTx(t testing.TB) (*database.Database, *database.Transaction, func()) {
	t.Helper()

	db := NewTestDB(t)
	conn := NewTestConn(t, db)

	tx, err := conn.BeginTx(&database.TxOptions{
		ReadOnly: false,
	})
	require.NoError(t, err)

	return db, tx, func() {
		_ = tx.Rollback()
	}
}

func Exec(db *database.Database, tx *database.Transaction, q string, params ...environment.Param) error {
	res, err := Query(db, tx, q, params...)
	if err != nil {
		return err
	}
	defer res.Close()

	return res.Skip(context.Background())
}

func Query(db *database.Database, tx *database.Transaction, q string, params ...environment.Param) (*statement.Result, error) {
	pq, err := parser.ParseQuery(q)
	if err != nil {
		return nil, err
	}

	ctx := &query.Context{Ctx: context.Background(), DB: db, Conn: tx.Connection(), Params: params}

	return query.New(pq...).Run(ctx)
}

func MustExec(t *testing.T, db *database.Database, tx *database.Transaction, q string, params ...environment.Param) {
	t.Helper()

	err := Exec(db, tx, q, params...)
	require.NoError(t, err)
}

func MustQuery(t *testing.T, db *database.Database, tx *database.Transaction, q string, params ...environment.Param) *statement.Result {
	res, err := Query(db, tx, q, params...)
	require.NoError(t, err)
	return res
}
