package genji

import (
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/record"
)

const (
	schemaTableName = "__genji.schema"
)

// schemaStore manages the schema table. It provides several typed helpers
// that simplify common operations.
type schemaStore struct {
	store *Store
}

// newSchemaStore creates a schemaStore.
func newSchemaStore(db *DB) *schemaStore {
	return &schemaStore{
		store: NewStore(db, schemaTableName, nil, nil),
	}
}

// newSchemaStoreWithTx creates a schemaStore valid for the lifetime of the given transaction.
func newSchemaStoreWithTx(tx *Tx) *schemaStore {
	return &schemaStore{
		store: NewStoreWithTx(tx, schemaTableName, nil, nil),
	}
}

// Init makes sure the table exists. No error is returned if the table already exists.
func (s *schemaStore) Init() error {
	return s.store.Update(func(tx *Tx) error {
		err := tx.CreateTable(schemaTableName)
		if err == engine.ErrTableAlreadyExists {
			return nil
		}

		return err
	})
}

// Insert a record in the table and return the primary key.
func (s *schemaStore) Insert(tableName string, schema *record.Schema) (rowid []byte, err error) {
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
func (s *schemaStore) Get(tableName string) (*record.Schema, error) {
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

	if err != nil {
		return nil, err
	}

	return sr.Schema, err
}

// Replace the schema for tableName by the given one.
func (s *schemaStore) Replace(tableName string, schema *record.Schema) error {
	sr := record.SchemaRecord{
		Schema: schema,
	}
	return s.store.UpdateTable(func(t *Table) error {
		return t.Replace([]byte(tableName), &sr)
	})
}
