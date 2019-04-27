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
	store *Store
}

// NewSchemaStore creates a SchemaStore.
func NewSchemaStore(db *DB) *SchemaStore {
	return &SchemaStore{
		store: NewStore(db, schemaTableName, nil),
	}
}

// NewSchemaStoreWithTx creates a SchemaStore valid for the lifetime of the given transaction.
func NewSchemaStoreWithTx(tx *Tx) *SchemaStore {
	return &SchemaStore{
		store: NewStoreWithTx(tx, schemaTableName, nil),
	}
}

// Init makes sure the table exists. No error is returned if the table already exists.
func (s *SchemaStore) Init() error {
	return s.store.Update(func(tx *Tx) error {
		err := tx.CreateTable(schemaTableName)
		if err == engine.ErrTableAlreadyExists {
			return nil
		}

		return err
	})
}

// Insert a record in the table and return the primary key.
func (s *SchemaStore) Insert(schema *record.Schema) (rowid []byte, err error) {
	err = s.store.UpdateTable(func(t table.Table) error {
		rowid, err = t.Insert(&record.SchemaRecord{Schema: schema, TableName: schemaTableName})
		return err
	})
	return
}

// Get a schema using its table name.
func (s *SchemaStore) Get(tableName string) (*record.Schema, error) {
	sr := record.SchemaRecord{
		Schema: new(record.Schema),
	}

	err := s.store.ViewTable(func(t table.Table) error {
		rec, err := t.Record([]byte(tableName))
		if err != nil {
			return err
		}

		return sr.ScanRecord(rec)
	})

	return sr.Schema, err
}
