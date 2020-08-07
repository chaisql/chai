// Package database provides database primitives such as tables, transactions and indexes.
package database

import (
	"sync"

	"github.com/genjidb/genji/engine"
)

// A Database manages a list of tables in an engine.
type Database struct {
	ng engine.Engine

	mu sync.Mutex

	// tableInfoStore manages information about all the tables
	tableInfoStore *tableInfoStore

	// tableDocids holds the latest docid for a table.
	// it is cached in this map the first time a table is accessed
	// and is used by every call to table#Insert to generate the
	// docid if the table doesn't have a primary key.
	tableDocids map[string]int64
}

// New initializes the DB using the given engine.
func New(ng engine.Engine) (*Database, error) {
	db := Database{
		ng:          ng,
		tableDocids: make(map[string]int64),
	}

	ntx, err := db.ng.Begin(true)
	if err != nil {
		return nil, err
	}
	defer ntx.Rollback()

	err = db.initInternalStores(ntx)
	if err != nil {
		return nil, err
	}

	db.tableInfoStore, err = newTableInfoStore(ntx)
	if err != nil {
		return nil, err
	}

	err = ntx.Commit()
	if err != nil {
		return nil, err
	}

	return &db, nil
}

func (db *Database) initInternalStores(tx engine.Transaction) error {
	_, err := tx.GetStore([]byte(tableInfoStoreName))
	if err == engine.ErrStoreNotFound {
		err = tx.CreateStore([]byte(tableInfoStoreName))
	}
	if err != nil {
		return err
	}

	_, err = tx.GetStore([]byte(indexStoreName))
	if err == engine.ErrStoreNotFound {
		err = tx.CreateStore([]byte(indexStoreName))
	}
	return err
}

// Close the underlying engine.
func (db *Database) Close() error {
	return db.ng.Close()
}

// Begin starts a new transaction.
// The returned transaction must be closed either by calling Rollback or Commit.
func (db *Database) Begin(writable bool) (*Transaction, error) {
	ntx, err := db.ng.Begin(writable)
	if err != nil {
		return nil, err
	}

	tx := Transaction{
		db:             db,
		Tx:             ntx,
		writable:       writable,
		tableInfoStore: db.tableInfoStore,
	}

	tx.indexStore, err = tx.getIndexStore()
	if err != nil {
		return nil, err
	}

	return &tx, nil
}

// GetTableInfo returns a map containing information about all the tables.
func (db *Database) GetTableInfo() map[string]TableInfo {
	return db.tableInfoStore.GetTableInfo()
}
