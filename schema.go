package genji

import (
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/record"
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
func (s *SchemaStore) Insert(tableName string, schema *record.Schema) (rowid []byte, err error) {
	err = s.store.Update(func(tx *Tx) error {
		t, err := tx.Transaction.Table(schemaTableName, record.NewCodec())
		if err != nil {
			return err
		}

		rowid, err = t.Insert(&record.SchemaRecord{Schema: schema, TableName: tableName})
		return err
	})
	return
}

// Get a schema using its table name.
func (s *SchemaStore) Get(tableName string) (*record.Schema, error) {
	sr := record.SchemaRecord{
		Schema: new(record.Schema),
	}

	err := s.store.View(func(tx *Tx) error {
		t, err := tx.Transaction.Table(schemaTableName, record.NewCodec())
		if err != nil {
			return err
		}

		rec, err := t.Record([]byte(tableName))
		if err != nil {
			return err
		}

		return sr.ScanRecord(rec)
	})

	return sr.Schema, err
}
