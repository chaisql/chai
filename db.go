package genji

import (
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/index"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

type DB struct {
	engine.Engine
}

func Open(ng engine.Engine) (*DB, error) {
	return &DB{Engine: ng}, nil
}

func (db DB) Begin(writable bool) (*Transaction, error) {
	tx, err := db.Engine.Begin(writable)
	if err != nil {
		return nil, err
	}

	return &Transaction{
		Transaction: tx,
	}, nil
}

type Transaction struct {
	engine.Transaction
}

func (tx Transaction) Table(name string) (table.Table, error) {
	tb, err := tx.Transaction.Table(name)
	if err != nil {
		return nil, err
	}

	indexes, err := tx.Transaction.Indexes(name)
	if err != nil {
		return nil, err
	}

	return &Table{
		Table:   tb,
		tx:      tx.Transaction,
		indexes: indexes,
	}, nil
}

func (tx Transaction) CreateTable(name string) (table.Table, error) {
	return tx.Transaction.CreateTable(name)
}

type Table struct {
	table.Table
	tx      engine.Transaction
	indexes map[string]index.Index
}

func (t Table) Insert(r record.Record) ([]byte, error) {
	rowid, err := t.Table.Insert(r)
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
