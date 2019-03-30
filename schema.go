package genji

import (
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

const (
	schemaTableName = "__genji.schema"
)

// SchemaStore manages the schema table. It provides several typed helpers
// that simplify common operations.
type SchemaStore struct {
	TxRunner
	TableTxRunner
}

// NewSchemaStore creates a SchemaStore.
func NewSchemaStore(db *DB) *SchemaStore {
	return &SchemaStore{
		TxRunner:      db,
		TableTxRunner: NewTableTxRunner(db, schemaTableName),
	}
}

// NewSchemaStoreWithTx creates a SchemaStore valid for the lifetime of the given transaction.
func NewSchemaStoreWithTx(tx *Tx) *SchemaStore {
	txp := TxRunnerProxy{Tx: tx}

	return &SchemaStore{
		TxRunner:      &txp,
		TableTxRunner: NewTableTxRunner(&txp, schemaTableName),
	}
}

// Init makes sure the table exists. No error is returned if the table already exists.
func (s *SchemaStore) Init() error {
	return s.Update(func(tx *Tx) error {
		var err error
		_, err = tx.CreateTable(schemaTableName)
		if err == engine.ErrTableAlreadyExists {
			return nil
		}

		return err
	})
}

// Insert a record in the table and return the primary key.
func (s *SchemaStore) Insert(record *record.Schema) (rowid []byte, err error) {
	err = s.UpdateTable(func(t table.Table) error {
		rowid, err = t.Insert(record)
		return err
	})
	return
}

// Get a schema using its table name.
func (s *SchemaStore) Get(tableName string) (*record.Schema, error) {
	var record record.Schema

	err := s.ViewTable(func(t table.Table) error {
		rec, err := t.Record([]byte(tableName))
		if err != nil {
			return err
		}

		return record.ScanRecord(rec)
	})

	return &record, err
}

type StaticStore struct {
	TxRunner
	TableTxRunner

	tableName string
	schema    record.Schema
}

// NewStaticStore creates a StaticStore.
func NewStaticStore(db *DB, tableName string, schema record.Schema) *StaticStore {
	return &StaticStore{
		TxRunner:      db,
		TableTxRunner: NewTableTxRunner(db, tableName),
		tableName:     tableName,
		schema:        schema,
	}
}

// NewStaticStoreWithTx creates a StaticStore valid for the lifetime of the given transaction.
func NewStaticStoreWithTx(tx *Tx, tableName string, schema record.Schema) *StaticStore {
	txp := TxRunnerProxy{Tx: tx}

	return &StaticStore{
		tableName:     tableName,
		schema:        schema,
		TxRunner:      &txp,
		TableTxRunner: NewTableTxRunner(&txp, schemaTableName),
	}
}

// Init makes sure the table exists. No error is returned if the table already exists.
func (s *StaticStore) Init() error {
	return s.Update(func(tx *Tx) error {
		_, err := tx.CreateTable(s.tableName)
		if err == engine.ErrTableAlreadyExists {
			return nil
		}

		ss := NewSchemaStoreWithTx(tx)
		err = ss.Init()
		if err != nil {
			return err
		}

		_, err = ss.Insert(&s.schema)
		return err
	})
}

// Insert a record in the table and return the primary key.
func (s *StaticStore) Insert(r record.Record) (rowid []byte, err error) {
	err = s.UpdateTable(func(t table.Table) error {
		rowid, err = t.Insert(r)
		return err
	})
	return
}

// Get a record using its primary key.
func (s *StaticStore) Get(rowid []byte, scanner record.Scanner) error {
	return s.ViewTable(func(t table.Table) error {
		rec, err := t.Record(rowid)
		if err != nil {
			return err
		}

		return scanner.ScanRecord(rec)
	})
}
