package database

import (
	"sync"

	"github.com/asdine/genji/document"
	"github.com/asdine/genji/engine"
	"github.com/pkg/errors"
)

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

	_, err = ntx.Store(tableConfigStoreName)
	if err == engine.ErrStoreNotFound {
		err = ntx.CreateStore(tableConfigStoreName)
	}
	if err != nil {
		return nil, err
	}

	_, err = ntx.Store(indexStoreName)
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
		tx:       ntx,
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

type indexOptions struct {
	IndexName string
	TableName string
	FieldName string
	Unique    bool
}

// Field implements the field method of the document.Document interface.
func (i *indexOptions) GetByField(name string) (document.Value, error) {
	switch name {
	case "IndexName":
		return document.NewStringValue(i.IndexName), nil
	case "TableName":
		return document.NewStringValue(i.TableName), nil
	case "FieldName":
		return document.NewStringValue(i.FieldName), nil
	case "Unique":
		return document.NewBoolValue(i.Unique), nil
	}

	return document.Value{}, errors.New("unknown field")
}

// Iterate through all the fields one by one and pass each of them to the given function.
// It the given function returns an error, the iteration is interrupted.
func (i *indexOptions) Iterate(fn func(string, document.Value) error) error {
	var err error
	var v document.Value

	v, _ = i.GetByField("IndexName")
	err = fn("IndexName", v)
	if err != nil {
		return err
	}

	v, _ = i.GetByField("TableName")
	err = fn("TableName", v)
	if err != nil {
		return err
	}

	v, _ = i.GetByField("FieldName")
	err = fn("FieldName", v)
	if err != nil {
		return err
	}

	v, _ = i.GetByField("Unique")
	err = fn("Unique", v)
	if err != nil {
		return err
	}

	return nil
}

// ScanDocument extracts fields from record and assigns them to the struct fields.
// It implements the document.Scanner interface.
func (i *indexOptions) ScanDocument(rec document.Document) error {
	return rec.Iterate(func(f string, v document.Value) error {
		var err error

		switch f {
		case "IndexName":
			i.IndexName, err = v.ConvertToString()
		case "TableName":
			i.TableName, err = v.ConvertToString()
		case "FieldName":
			i.FieldName, err = v.ConvertToString()
		case "Unique":
			i.Unique, err = v.ConvertToBool()
		}
		return err
	})
}
