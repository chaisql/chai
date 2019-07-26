package genji

import (
	"errors"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
)

const (
	schemaTableName = "__genji.schema"
)

// schemaStore manages the schema table. It provides several typed helpers
// that simplify common operations.
type schemaStore struct {
	tx *Tx
}

// newSchemaStoreWithTx creates a schemaStore valid for the lifetime of the given transaction.
func newSchemaStoreWithTx(tx *Tx) *schemaStore {
	return &schemaStore{
		tx: tx,
	}
}

// Init makes sure the table exists. No error is returned if the table already exists.
func (s *schemaStore) Init() error {
	err := s.tx.CreateTable(schemaTableName)
	if err == engine.ErrTableAlreadyExists {
		return nil
	}
	return err
}

// Insert a record in the table and return the primary key.
func (s *schemaStore) Insert(tableName string, schema *record.Schema) ([]byte, error) {
	t, err := s.tx.Transaction.Table(schemaTableName, record.NewCodec())
	if err != nil {
		return nil, err
	}

	return t.Insert(&schemaRecord{Schema: schema, TableName: tableName})
}

// Get a schema using its table name.
func (s *schemaStore) Get(tableName string) (*record.Schema, error) {
	sr := schemaRecord{
		Schema: new(record.Schema),
	}

	t, err := s.tx.Transaction.Table(schemaTableName, record.NewCodec())
	if err != nil {
		return nil, err
	}

	rec, err := t.Record([]byte(tableName))
	if err != nil {
		return nil, err
	}

	err = sr.ScanRecord(rec)
	if err != nil {
		return nil, err
	}

	return sr.Schema, nil
}

// Replace the schema for tableName by the given one.
func (s *schemaStore) Replace(tableName string, schema *record.Schema) error {
	sr := schemaRecord{
		Schema: schema,
	}

	t, err := s.tx.Table(tableName)
	if err != nil {
		return err
	}

	return t.Replace([]byte(tableName), &sr)
}

type schemaRecord struct {
	*record.Schema
	TableName string
}

// Pk returns the TableName as the primary key.
func (s *schemaRecord) Pk() ([]byte, error) {
	return []byte(s.TableName), nil
}

// Field implements the field method of the Record interface.
func (s *schemaRecord) Field(name string) (field.Field, error) {
	switch name {
	case "TableName":
		return field.Field{
			Name: "TableName",
			Type: field.String,
			Data: []byte(s.TableName),
		}, nil
	case "Fields":
		data, err := record.Encode(s.Fields)
		if err != nil {
			return field.Field{}, err
		}

		return field.Field{
			Name: "Fields",
			Type: field.String,
			Data: data,
		}, nil
	}

	return field.Field{}, errors.New("unknown field")
}

// Iterate through all the fields one by one and pass each of them to the given function.
// It the given function returns an error, the iteration is interrupted.
func (s *schemaRecord) Iterate(fn func(field.Field) error) error {
	var err error
	var f field.Field

	f, err = s.Field("TableName")
	if err != nil {
		return err
	}

	err = fn(f)
	if err != nil {
		return err
	}

	f, err = s.Field("Fields")
	if err != nil {
		return err
	}

	err = fn(f)
	if err != nil {
		return err
	}

	return nil
}

// ScanRecord extracts fields from record and assigns them to the struct fields.
func (s *schemaRecord) ScanRecord(rec record.Record) error {
	var f field.Field
	var err error

	f, err = rec.Field("TableName")
	if err != nil {
		return err
	}
	s.TableName = string(f.Data)

	f, err = rec.Field("Fields")
	if err != nil {
		return err
	}

	if s.Schema == nil {
		s.Schema = new(record.Schema)
	}

	ec := record.EncodedRecord(f.Data)
	return ec.Iterate(func(f field.Field) error {
		s.Fields = append(s.Fields, f)
		return nil
	})
}
