package testutil

import (
	"context"
	kvpebble "github.com/genjidb/genji/internal/kv/pebble"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/database/catalogstore"
	ipebble "github.com/genjidb/genji/internal/database/pebble"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/kv"
	"github.com/genjidb/genji/internal/query"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
)

func TempDir(t testing.TB) string {
	dir, err := ioutil.TempDir("", "genji")
	assert.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return dir
}

func NewPebble(t testing.TB) *pebble.DB {
	t.Helper()

	dir := TempDir(t)

	db, err := ipebble.Open(filepath.Join(dir, "pebble"), nil)
	assert.NoError(t, err)

	t.Cleanup(func() {
		db.Close()
	})
	return db
}

func NewMemPebble(t testing.TB) *pebble.DB {
	t.Helper()

	pdb, err := ipebble.Open("", &pebble.Options{FS: vfs.NewStrictMem()})
	assert.NoError(t, err)

	return pdb
}

func NewTestTree(t testing.TB, namespace tree.Namespace) *tree.Tree {
	t.Helper()

	pdb := NewMemPebble(t)

	session := kvpebble.NewStore(pdb, kv.Options{
		RollbackSegmentNamespace: int64(database.RollbackSegmentNamespace),
		MaxBatchSize:             1 << 7,
	}).NewBatchSession()

	t.Cleanup(func() {
		session.Close()
		pdb.Close()
	})

	return tree.New(session, namespace)
}

func NewTestDB(t testing.TB) *database.Database {
	t.Helper()

	return NewTestDBWithPebble(t, NewMemPebble(t))
}

func NewTestDBWithPebble(t testing.TB, pdb *pebble.DB) *database.Database {
	t.Helper()

	db, err := database.New(kvpebble.NewStore(pdb, kv.Options{
		RollbackSegmentNamespace: int64(database.RollbackSegmentNamespace),
	}))
	assert.NoError(t, err)

	sess := db.Store.NewSnapshotSession()
	defer sess.Close()
	err = catalogstore.LoadCatalog(sess, db.Catalog)
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
	t.Helper()

	err := Exec(db, tx, q, params...)
	assert.NoError(t, err)
}

func MustQuery(t *testing.T, db *database.Database, tx *database.Transaction, q string, params ...environment.Param) *statement.Result {
	res, err := Query(db, tx, q, params...)
	assert.NoError(t, err)
	return res
}
