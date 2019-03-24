package genji

import (
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

// SchemaTable manages the schema table. It provides several typed helpers
// that simplify common operations.
type SchemaTable struct {
	TxRunner
	TableTxRunner
}

// NewSchemaTable creates a SchemaTable.
func NewSchemaTable(db *DB) *SchemaTable {
	return &SchemaTable{
		TxRunner:      db,
		TableTxRunner: NewTableTxRunner(db, "Schema"),
	}
}

// NewSchemaTableWithTx creates a SchemaTable valid for the lifetime of the given transaction.
func NewSchemaTableWithTx(tx *Tx) *SchemaTable {
	txp := TxRunnerProxy{Tx: tx}

	return &SchemaTable{
		TxRunner:      &txp,
		TableTxRunner: NewTableTxRunner(&txp, "Schema"),
	}
}

// Init makes sure the database exists. No error is returned if the database already exists.
func (b *SchemaTable) Init() error {
	return b.Update(func(tx *Tx) error {
		var err error
		_, err = tx.CreateTable("Schema")
		if err == engine.ErrTableAlreadyExists {
			return nil
		}

		return err
	})
}

// Insert a record in the table and return the primary key.
func (b *SchemaTable) Insert(record *record.Schema) (rowid []byte, err error) {
	err = b.UpdateTable(func(t table.Table) error {
		rowid, err = t.Insert(record)
		return err
	})
	return
}

// Get a record using its primary key.
func (b *SchemaTable) Get(rowid []byte) (*record.Schema, error) {
	var record record.Schema

	err := b.ViewTable(func(t table.Table) error {
		rec, err := t.Record(rowid)
		if err != nil {
			return err
		}

		return record.ScanRecord(rec)
	})

	return &record, err
}
