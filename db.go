package genji

import (
	"bytes"
	"fmt"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

// DB represents a collection of tables stored in the underlying engine.
// DB differs from the engine in that it provides automatic indexing, support for schemas
// and database administration methods.
// DB is safe for concurrent use unless the given engine isn't.
type DB struct {
	engine.Engine
}

// New initializes the DB using the given engine.
// It creates the schema table.
func New(ng engine.Engine) (*DB, error) {
	db := DB{
		Engine: ng,
	}

	err := newSchemaStore(&db).Init()
	if err != nil {
		return nil, err
	}

	return &db, nil
}

// Begin starts a new transaction.
// The returned transaction must be closed either by calling Rollback or Commit.
func (db DB) Begin(writable bool) (*Tx, error) {
	tx, err := db.Engine.Begin(writable)
	if err != nil {
		return nil, err
	}

	gtx := Tx{
		Transaction: tx,
	}
	gtx.schemas = newSchemaStoreWithTx(&gtx)
	return &gtx, nil
}

// View starts a read only transaction, runs fn and automatically rolls it back.
func (db DB) View(fn func(tx *Tx) error) error {
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	return fn(tx)
}

// Update starts a read-write transaction, runs fn and automatically commits it.
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

// Tx represents a database transaction. It provides methods for managing the
// collection of tables and the transaction itself.
// Tx is either read-only or read/write. Read-only can be used to read tables
// and read/write can be used to read, create, delete and modify tables.
type Tx struct {
	engine.Transaction

	schemas *schemaStore
}

// CreateTableWithSchema creates a table and associates the schema to it.
// These tables are more stricts than schemaless ones, any inserted record will be
// validated against that schema, making sure all records have the same fields.
func (tx Tx) CreateTableWithSchema(name string, schema *record.Schema) error {
	err := tx.Transaction.CreateTable(name)
	if err != nil {
		return err
	}

	_, err = tx.schemas.Insert(name, schema)
	return err
}

// Table returns a table by name. The table instance is only valid for the lifetime of the transaction.
func (tx Tx) Table(name string) (table.Table, error) {
	tb, err := tx.Transaction.Table(name, record.NewCodec())
	if err != nil {
		return nil, err
	}

	schema, err := tx.schemas.Get(name)
	if err != nil {
		if err != table.ErrRecordNotFound {
			return nil, err
		}

		schema = nil
	}

	return &Table{
		Table:   tb,
		tx:      tx.Transaction,
		name:    name,
		schema:  schema,
		schemas: tx.schemas,
	}, nil
}

// A Table represents a collection of records.
type Table struct {
	table.Table

	tx      engine.Transaction
	name    string
	schema  *record.Schema
	schemas *schemaStore
}

// Insert the record into the table.
// If the table is schemaful, the record is first validated against the schema,
// and an error is returned if there is a mismatch.
// Indexes are automatically updated.
func (t Table) Insert(r record.Record) ([]byte, error) {
	if t.schema != nil {
		err := t.schema.Validate(r)
		if err != nil {
			return nil, err
		}
	}

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

// Delete a record by rowid.
// Indexes are automatically updated.
func (t Table) Delete(rowid []byte) error {
	err := t.Table.Delete(rowid)
	if err != nil {
		return err
	}

	indexes, err := t.tx.Indexes(t.name)
	if err != nil {
		return err
	}

	for _, idx := range indexes {
		err = idx.Delete(rowid)
		if err != nil {
			return err
		}
	}

	return nil
}

// Replace a record by rowid. If the table is schemaful, r must match the schema.
// An error is returned if the rowid doesn't exist.
// Indexes are automatically updated.
func (t Table) Replace(rowid []byte, r record.Record) error {
	if t.schema != nil {
		err := t.schema.Validate(r)
		if err != nil {
			return err
		}
	}

	err := t.Table.Replace(rowid, r)
	if err != nil {
		return err
	}

	indexes, err := t.tx.Indexes(t.name)
	if err != nil {
		return err
	}

	for fieldName, idx := range indexes {
		f, err := r.Field(fieldName)
		if err != nil {
			return err
		}

		err = idx.Set(f.Data, rowid)
		if err != nil {
			return err
		}
	}

	return nil
}

// AddField changes the table structure by adding a field to all the records.
// If the field data is empty, it is filled with the zero value of the field type.
// Returns an error if the field already exists.
func (t Table) AddField(f field.Field) error {
	if t.schema != nil {
		if _, err := t.schema.Fields.Field(f.Name); err == nil {
			return fmt.Errorf("field %q already exists", f.Name)
		}
	}

	t.schema.Fields.Add(f)

	err := t.Table.Iterate(func(rowid []byte, r record.Record) error {
		var fb record.FieldBuffer
		err := fb.ScanRecord(r)
		if err != nil {
			return err
		}

		if _, err = fb.Field(f.Name); err == nil {
			return fmt.Errorf("field %q already exists", f.Name)
		}

		if f.Data == nil {
			f.Data = field.ZeroValue(f.Type).Data
		}
		fb.Add(f)
		return t.Table.Replace(rowid, fb)
	})
	if err != nil {
		return err
	}

	if t.schema == nil {
		return nil
	}

	t.schema.Fields.Add(f)
	return t.schemas.Replace(t.name, t.schema)
}

// DeleteField changes the table structure by deleting a field from all the records.
// If a schema is used, returns an error if the field doesn't exists.
func (t Table) DeleteField(name string) error {
	if t.schema != nil {
		if _, err := t.schema.Fields.Field(name); err != nil {
			return fmt.Errorf("field %q doesn't exists", name)
		}
	}

	err := t.Table.Iterate(func(rowid []byte, r record.Record) error {
		var fb record.FieldBuffer
		err := fb.ScanRecord(r)
		if err != nil {
			return err
		}

		err = fb.Delete(name)
		if err != nil {
			// if the field doesn't exist, skip
			return nil
		}

		return t.Table.Replace(rowid, fb)
	})
	if err != nil {
		return err
	}

	if t.schema == nil {
		return nil
	}

	err = t.schema.Fields.Delete(name)
	if err != nil {
		return err
	}

	return t.schemas.Replace(t.name, t.schema)
}

// RenameField changes the table structure by renaming the selected field on all the records.
// If a schema is used, returns an error if the field doesn't exists.
func (t Table) RenameField(oldName, newName string) error {
	var sf field.Field
	var err error

	if t.schema != nil {
		if sf, err = t.schema.Fields.Field(oldName); err != nil {
			return fmt.Errorf("field %q doesn't exists", oldName)
		}
	}

	err = t.Table.Iterate(func(rowid []byte, r record.Record) error {
		var fb record.FieldBuffer
		err := fb.ScanRecord(r)
		if err != nil {
			return err
		}

		f, err := fb.Field(oldName)
		if err != nil {
			// if the field doesn't exist, skip
			return nil
		}

		f.Name = newName
		fb.Replace(oldName, f)
		return t.Table.Replace(rowid, fb)
	})
	if err != nil {
		return err
	}

	if t.schema == nil {
		return nil
	}

	sf.Name = newName
	t.schema.Fields.Replace(oldName, sf)
	return t.schemas.Replace(t.name, t.schema)
}

// String displays the table as a csv compatible string.
func (t Table) String() string {
	var buf bytes.Buffer

	if t.schema != nil {
		fmt.Fprintf(&buf, "%s\n", t.schema.String())
	}

	err := t.Iterate(func(rowid []byte, r record.Record) error {
		first := true
		err := r.Iterate(func(f field.Field) error {
			if !first {
				buf.WriteString(", ")
			}
			first = false

			v, err := field.Decode(f)
			if t.schema != nil {
				fmt.Fprintf(&buf, "%#v", v)
			} else {
				fmt.Fprintf(&buf, "%s(%s): %#v", f.Name, f.Type, v)
			}
			return err
		})
		if err != nil {
			return err
		}

		fmt.Fprintf(&buf, "\n")
		return nil
	})

	if err != nil {
		return err.Error()
	}

	return buf.String()
}
