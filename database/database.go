package database

import (
	"sync"

	"github.com/asdine/genji/document"
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/value"
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
func (i *indexOptions) GetValueByName(name string) (document.Field, error) {
	switch name {
	case "IndexName":
		return document.NewStringValue("IndexName", i.IndexName), nil
	case "TableName":
		return document.NewStringValue("TableName", i.TableName), nil
	case "FieldName":
		return document.NewStringValue("FieldName", i.FieldName), nil
	case "Unique":
		return document.NewBoolValue("Unique", i.Unique), nil
	}

	return document.Field{}, errors.New("unknown field")
}

// Iterate through all the fields one by one and pass each of them to the given function.
// It the given function returns an error, the iteration is interrupted.
func (i *indexOptions) Iterate(fn func(document.Field) error) error {
	var err error
	var f document.Field

	f, _ = i.GetValueByName("IndexName")
	err = fn(f)
	if err != nil {
		return err
	}

	f, _ = i.GetValueByName("TableName")
	err = fn(f)
	if err != nil {
		return err
	}

	f, _ = i.GetValueByName("FieldName")
	err = fn(f)
	if err != nil {
		return err
	}

	f, _ = i.GetValueByName("Unique")
	err = fn(f)
	if err != nil {
		return err
	}

	return nil
}

// ScanRecord extracts fields from record and assigns them to the struct fields.
// It implements the document.Scanner interface.
func (i *indexOptions) ScanRecord(rec document.Document) error {
	return rec.Iterate(func(f document.Field) error {
		var err error

		switch f.Name {
		case "IndexName":
			i.IndexName, err = value.DecodeString(f.Data)
		case "TableName":
			i.TableName, err = value.DecodeString(f.Data)
		case "FieldName":
			i.FieldName, err = value.DecodeString(f.Data)
		case "Unique":
			i.Unique, err = value.DecodeBool(f.Data)
		}
		return err
	})
}
