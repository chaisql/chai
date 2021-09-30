package database

import (
	"context"
	"sync"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
	"github.com/genjidb/genji/engine"
)

const maxTransientPoolSize = 3

// TransientDatabasePool manages a pool of transient databases.
// It keeps a pool of maxTransientPoolSize databases.
type TransientDatabasePool struct {
	ng    engine.Engine
	codec encoding.Codec

	mu   sync.Mutex
	pool []*Database
}

// Get returns a free engine from the pool, if any. Otherwise it creates a new engine
// and returns it.
func (t *TransientDatabasePool) Get(ctx context.Context) (*Database, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.pool) > 0 {
		ng := t.pool[len(t.pool)-1]
		t.pool = t.pool[:len(t.pool)-1]
		return ng, nil
	}

	tng, err := t.ng.NewTransientEngine(ctx)
	if err != nil {
		return nil, err
	}

	tdb, err := New(ctx, tng, Options{Codec: t.codec})
	if err != nil {
		_ = tng.Close()
		return nil, err
	}

	return tdb, nil
}

// Release sets the engine for reuse. If the pool is full, it drops the given engine.
func (t *TransientDatabasePool) Release(ctx context.Context, db *Database) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.pool) >= maxTransientPoolSize {
		err := db.Close()
		if err != nil {
			return err
		}

		return db.ng.Drop(ctx)
	}

	t.pool = append(t.pool, db)
	return nil
}

// TempResources holds a temporary database table, and
// optionally index.
type TempResources struct {
	DB    *Database
	Tx    *Transaction
	Table *Table
	Index *Index
}

// NewTransientTable creates a temporary database and table.
func NewTransientTable(db *Database, tableName string) (*TempResources, func() error, error) {
	tdb, cleanup, err := db.NewTransientDB(context.Background())
	if err != nil {
		return nil, nil, err
	}

	// create a write transaction that will be rolled back when the stream is over
	ttx, err := tdb.Begin(true)
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	f := func() error {
		rerr := ttx.Rollback()
		cerr := cleanup()
		if rerr != nil {
			return rerr
		}
		return cerr
	}

	defer func() {
		if err != nil {
			f()
		}
	}()

	// create a temporary table
	err = tdb.Catalog.CreateTable(ttx, tableName, nil)
	if err != nil {
		return nil, nil, err
	}

	// get the temporary tempTable
	tempTable, err := tdb.Catalog.GetTable(ttx, tableName)
	if err != nil {
		return nil, nil, err
	}

	return &TempResources{
		DB:    tdb,
		Table: tempTable,
		Tx:    ttx,
	}, f, nil
}

// NewTransientIndex creates a temporary database, table and index.
func NewTransientIndex(db *Database, tableName string, paths []document.Path, unique bool) (temp *TempResources, cleanup func() error, err error) {
	temp, cleanup, err = NewTransientTable(db, tableName)
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			cleanup()
		}
	}()

	// Create an index with no name.
	// The catalog will generate a name and set it to
	// the idxInfo IndexName field
	idxInfo := &IndexInfo{
		TableName: tableName,
		Paths:     paths,
		Unique:    unique,
	}
	err = temp.DB.Catalog.CreateIndex(temp.Tx, idxInfo)
	if err != nil {
		return
	}
	temp.Index, err = temp.DB.Catalog.GetIndex(temp.Tx, idxInfo.IndexName)
	return
}
