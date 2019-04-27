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

type Tx struct {
	engine.Transaction

	schemas *SchemaStore
}

func (tx Tx) CreateTableWithSchema(name string, schema *record.Schema) error {
	err := tx.Transaction.CreateTable(name)
	if err != nil {
		return err
	}

	_, err = tx.schemas.Insert(schema)
	return err
}

func (tx Tx) Table(name string, codec record.Codec) (table.Table, error) {
	tb, err := tx.Transaction.Table(name, codec)
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
		Table:  tb,
		tx:     tx.Transaction,
		name:   name,
		schema: schema,
	}, nil
}

type Table struct {
	table.Table

	tx     engine.Transaction
	name   string
	schema *record.Schema
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
