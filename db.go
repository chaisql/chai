package genji

import (
	"fmt"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/field"
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

	err := NewSchemaStore(&db).Init()
	if err != nil {
		return nil, err
	}

	return &db, nil
}

func (db DB) Begin(writable bool) (*Tx, error) {
	tx, err := db.Engine.Begin(writable)
	if err != nil {
		return nil, err
	}

	gtx := Tx{
		Transaction: tx,
	}
	gtx.schemas = NewSchemaStoreWithTx(&gtx)
	return &gtx, nil
}

func (db DB) View(fn func(tx *Tx) error) error {
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	return fn(tx)
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

	schemas *SchemaStore
}

func (tx Tx) CreateTableWithSchema(name string, schema *record.Schema) error {
	err := tx.Transaction.CreateTable(name)
	if err != nil {
		return err
	}

	_, err = tx.schemas.Insert(name, schema)
	return err
}

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

type Table struct {
	table.Table

	tx      engine.Transaction
	name    string
	schema  *record.Schema
	schemas *SchemaStore
}

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
// Returns an error if the field doesn't exists.
func (t Table) DeleteField(name string) error {
	err := t.Table.Iterate(func(rowid []byte, r record.Record) error {
		var fb record.FieldBuffer
		err := fb.ScanRecord(r)
		if err != nil {
			return err
		}

		err = fb.Delete(name)
		if err != nil {
			return err
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
