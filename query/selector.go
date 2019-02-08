package query

import (
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
)

type Field string

func (f Field) Name() string {
	return string(f)
}

func (f Field) SelectField(r record.Record) (field.Field, error) {
	return r.Field(string(f))
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
