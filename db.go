package genji

import (
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

type DB struct {
	engine.Engine
}

func New(ng engine.Engine) *DB {
	return &DB{Engine: ng}
}

func (db DB) Begin(writable bool) (*Tx, error) {
	tx, err := db.Engine.Begin(writable)
	if err != nil {
		return nil, err
	}

	return &Tx{
		Transaction: tx,
	}, nil
}

type Tx struct {
	engine.Transaction
}

func (tx Tx) Table(name string) (table.Table, error) {
	tb, err := tx.Transaction.Table(name)
	if err != nil {
		return nil, err
	}

	return &Table{
		Table: tb,
		tx:    tx.Transaction,
		name:  name,
	}, nil
}

func (tx Tx) CreateTable(name string) (table.Table, error) {
	tb, err := tx.Transaction.CreateTable(name)
	if err != nil {
		return nil, err
	}

	return &Table{
		Table: tb,
		tx:    tx.Transaction,
		name:  name,
	}, nil
}

type Table struct {
	table.Table

	tx   engine.Transaction
	name string
}

func (t Table) Insert(r record.Record) ([]byte, error) {
	rowid, err := t.Table.Insert(r)
	if err != nil {
		return nil, err
	}

	indexes, err := t.tx.Indexes(t.name)
	if err != nil {
		return nil, err
	}

	for fieldName, idx := range indexes {
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
