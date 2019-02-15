package genji

import (
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

type DB struct {
	ng engine.Engine
}

func Open(ng engine.Engine) (*DB, error) {
	return &DB{ng: ng}, nil
}

func (db DB) Begin(writable bool) (*Transaction, error) {
	tx, err := db.ng.Begin(writable)
	if err != nil {
		return nil, err
	}

	return &Transaction{
		tx: tx,
	}, nil
}

type Transaction struct {
	tx engine.Transaction
}

func (tx Transaction) Rollback() error {
	return tx.tx.Rollback()
}

func (tx Transaction) Commit() error {
	return tx.tx.Commit()
}

func (tx Transaction) Table(name string) (table.Table, error) {
	return tx.tx.Table(name)
}

func (tx Transaction) CreateTable(name string) (table.Table, error) {
	return tx.tx.CreateTable(name)
}

func (tx Transaction) Index(table, name string) (index.Index, error) {
	return tx.tx.Index(table, name)
}

func (tx Transaction) CreateIndex(table, name string) (index.Index, error) {
	return tx.tx.CreateIndex(table, name)
}

type Table struct {
	table   table.Table
	tx      engine.Transaction
	indexes map[string]index.Index
}

func (t Table) Insert(r record.Record) ([]byte, error) {
	rowid, err := t.table.Insert(r)
	if err != nil {
		return nil, err
	}

	for fieldName, idx := range t.indexes {
		f, err := r.Field(fieldName)
		if err != nil {
			return nil, err
		}

		err = idx.Set(f.Data, rowid)
		if err != nil {
			return nil, err
		}
	}

	return rowid, nil
}

func (t Table) Iterate(func(record.Record) bool) error {
	return nil
}

func (t Table) Record(rowid []byte) (record.Record, error) {
	return nil, nil
}
