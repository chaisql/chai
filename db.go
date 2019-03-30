package genji

import (
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

type DB struct {
	engine.Engine
}

func New(ng engine.Engine) (*DB, error) {
	db := DB{
		Engine: ng,
	}

	return &db, nil
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

func (db DB) View(fn func(tx *Tx) error) error {
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = fn(tx)
	if err != nil {
		return err
	}

	return tx.Rollback()
}

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

type TxRunner interface {
	View(func(*Tx) error) error
	Update(func(*Tx) error) error
}

type TableTxRunner interface {
	ViewTable(func(table.Table) error) error
	UpdateTable(func(table.Table) error) error
}

type tableTxRunner struct {
	txer      TxRunner
	tableName string
}

func NewTableTxRunner(txer TxRunner, tableName string) TableTxRunner {
	return &tableTxRunner{txer: txer, tableName: tableName}
}

func (t *tableTxRunner) ViewTable(fn func(table.Table) error) error {
	return t.txer.View(func(tx *Tx) error {
		tb, err := tx.Table(t.tableName)
		if err != nil {
			return err
		}

		return fn(tb)
	})
}

func (t *tableTxRunner) UpdateTable(fn func(table.Table) error) error {
	return t.txer.Update(func(tx *Tx) error {
		tb, err := tx.Table(t.tableName)
		if err != nil {
			return err
		}

		return fn(tb)
	})
}

type TxRunnerProxy struct {
	Tx *Tx
}

func (t *TxRunnerProxy) View(fn func(*Tx) error) error {
	return fn(t.Tx)
}

func (t *TxRunnerProxy) Update(fn func(*Tx) error) error {
	return fn(t.Tx)
}
