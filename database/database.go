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
}

// New initializes the DB using the given engine.
func New(ng engine.Engine) (*Database, error) {
	db := Database{
		ng: ng,
	}

	ntx, err := db.ng.Begin(true)
	if err != nil {
		return nil, err
	}
	defer ntx.Rollback()

	_, err = ntx.GetStore(tableConfigStoreName)
	if err == engine.ErrStoreNotFound {
		err = ntx.CreateStore(tableConfigStoreName)
	}
	if err != nil {
		return nil, err
	}

	_, err = ntx.GetStore(indexStoreName)
	if err == engine.ErrStoreNotFound {
		err = ntx.CreateStore(indexStoreName)
	}
	if err != nil {
		return nil, err
	}

	err = ntx.Commit()
	if err != nil {
		return nil, err
	}

	return &db, nil
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
		db:       db,
		Tx:       ntx,
		writable: writable,
	}

	tx.tcfgStore, err = tx.getTableConfigStore()
	if err != nil {
		return nil, err
	}

	tx.indexStore, err = tx.getIndexStore()
	if err != nil {
		return nil, err
	}

	return &tx, nil
}
