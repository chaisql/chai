package schema

import (
	"errors"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

// Schema contains information about a table and its fields.
type Schema struct {
	TableName string
	Fields    string
}

// Field implements the field method of the record.Record interface.
func (s *Schema) Field(name string) (field.Field, error) {
	switch name {
	case "TableName":
		return field.Field{
			Name: "TableName",
			Type: field.String,
			Data: []byte(s.TableName),
		}, nil
	case "Fields":
		return field.Field{
			Name: "Fields",
			Type: field.String,
			Data: []byte(s.Fields),
		}, nil
	}

	return field.Field{}, errors.New("unknown field")
}

// Iterate through all the fields one by one and pass each of them to the given function.
// It the given function returns an error, the iteration is interrupted.
func (s *Schema) Iterate(fn func(field.Field) error) error {
	var err error
	var f field.Field

	f, _ = s.Field("TableName")
	err = fn(f)
	if err != nil {
		return err
	}

	f, _ = s.Field("Fields")
	err = fn(f)
	if err != nil {
		return err
	}

	return nil
}

// ScanRecord extracts fields from record and assigns them to the struct fields.
func (s *Schema) ScanRecord(rec record.Record) error {
	var f field.Field
	var err error

	f, err = rec.Field("TableName")
	if err == nil {
		s.TableName = string(f.Data)
	}

	f, err = rec.Field("Fields")
	if err == nil {
		s.Fields = string(f.Data)
	}

	return nil
}

// Table manages the schema table. It provides several typed helpers
// that simplify common operations.
type Table struct {
	genji.TxRunner
	genji.TableTxRunner
}

// NewTable creates a Table.
func NewTable(db *genji.DB) *Table {
	return &Table{
		TxRunner:      db,
		TableTxRunner: genji.NewTableTxRunner(db, "Schema"),
	}
}

// NewTableWithTx creates a Table valid for the lifetime of the given transaction.
func NewTableWithTx(tx *genji.Tx) *Table {
	txp := genji.TxRunnerProxy{Tx: tx}

	return &Table{
		TxRunner:      &txp,
		TableTxRunner: genji.NewTableTxRunner(&txp, "Schema"),
	}
}

// Init makes sure the database exists. No error is returned if the database already exists.
func (s *Table) Init() error {
	return s.Update(func(tx *genji.Tx) error {
		var err error
		_, err = tx.CreateTable("Schema")
		if err == engine.ErrTableAlreadyExists {
			return nil
		}

		return err
	})
}

// Insert a record in the table and return the primary key.
func (s *Table) Insert(record *Schema) (rowid []byte, err error) {
	err = s.UpdateTable(func(t table.Table) error {
		rowid, err = t.Insert(record)
		return err
	})
	return
}

// Get a record using its primary key.
func (s *Table) Get(rowid []byte) (*Schema, error) {
	var record Schema

	err := s.ViewTable(func(t table.Table) error {
		rec, err := t.Record(rowid)
		if err != nil {
			return err
		}

		return record.ScanRecord(rec)
	})

	return &record, err
}
