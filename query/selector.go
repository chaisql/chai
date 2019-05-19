package query

import (
	"github.com/asdine/genji"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

type FieldSelector interface {
	SelectField(record.Record) (field.Field, error)
	Name() string
	As(string) FieldSelector
}

type TableSelector interface {
	SelectTable(*genji.Tx) (table.Table, error)
	Name() string
}

type Field string

func (f Field) Name() string {
	return string(f)
}

func (f Field) SelectField(r record.Record) (field.Field, error) {
	return r.Field(string(f))
}

func (f Field) As(alias string) FieldSelector {
	return &Alias{FieldSelector: f, Alias: alias}
}

type Alias struct {
	FieldSelector
	Alias string
}

func (a Alias) Name() string {
	return a.FieldSelector.Name()
}

func (a Alias) SelectField(r record.Record) (field.Field, error) {
	f, err := a.FieldSelector.SelectField(r)
	if err != nil {
		return field.Field{}, err
	}

	return field.Field{
		Data: f.Data,
		Type: f.Type,
		Name: a.Alias,
	}, nil
}

type Table string

func (t Table) Name() string {
	return string(t)
}

func (t Table) SelectTable(tx *genji.Tx) (table.Table, error) {
	return tx.Table(string(t))
}
