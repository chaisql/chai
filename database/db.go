package database

import (
	"math/rand"
	"time"

	"github.com/asdine/genji/engine"
)

var (
	entropy          = rand.New(rand.NewSource(time.Now().UnixNano()))
	separator   byte = 0x1F
	indexTable       = "__genji.indexes"
	indexPrefix      = "i"
)

// DB represents a collection of tables stored in the underlying engine.
// DB differs from the engine in that it provides automatic indexing
// and database administration methods.
// DB is safe for concurrent use unless the given engine isn't.
type DB struct {
	ng engine.Engine
}

// New initializes the DB using the given engine.
func New(ng engine.Engine) (*DB, error) {
	db := DB{
		ng: ng,
	}

	err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateTableIfNotExists(indexTable)
		return err
	})
	if err != nil {
		return nil, err
	}

	return &db, nil
}

// Close the underlying engine.
func (db DB) Close() error {
	return db.ng.Close()
}

// Begin starts a new transaction.
// The returned transaction must be closed either by calling Rollback or Commit.
func (db DB) Begin(writable bool) (*Tx, error) {
	tx, err := db.ng.Begin(writable)
	if err != nil {
		return nil, err
	}

	return &Tx{
		tx: tx,
	}, nil
}

// View starts a read only transaction, runs fn and automatically rolls it back.
func (db DB) View(fn func(tx *Tx) error) error {
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	return fn(tx)
}

// Update starts a read-write transaction, runs fn and automatically commits it.
func (db DB) Update(fn func(tx *Tx) error) error {
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = fn(tx)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// ViewTable starts a read only transaction, fetches the selected table, calls fn with that table
// and automatically rolls back the transaction.
func (db DB) ViewTable(tableName string, fn func(*Tx, *Table) error) error {
	return db.View(func(tx *Tx) error {
		tb, err := tx.GetTable(tableName)
		if err != nil {
			return err
		}

		return fn(tx, tb)
	})
}

// UpdateTable starts a read/write transaction, fetches the selected table, calls fn with that table
// and automatically commits the transaction.
// If fn returns an error, the transaction is rolled back.
func (db DB) UpdateTable(tableName string, fn func(*Tx, *Table) error) error {
	return db.Update(func(tx *Tx) error {
		tb, err := tx.GetTable(tableName)
		if err != nil {
			return err
		}

		return fn(tx, tb)
	})
}
