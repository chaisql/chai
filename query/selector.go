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

type Int64Field struct {
	FieldSelector
}

func NewInt64Field(name string) Int64Field {
	return Int64Field{FieldSelector: Field(name)}
}

func (f Int64Field) Eq(v int) Matcher {
	return EqInt(f.FieldSelector, v)
}

func (f Int64Field) Gt(v int) Matcher {
	return GtInt(f.FieldSelector, v)
}

func (f Int64Field) Gte(v int) Matcher {
	return GteInt(f.FieldSelector, v)
}

func (f Int64Field) Lt(v int) Matcher {
	return LtInt(f.FieldSelector, v)
}

func (f Int64Field) Lte(v int) Matcher {
	return LteInt(f.FieldSelector, v)
}

type StrField struct {
	FieldSelector
}

func NewStrField(name string) StrField {
	return StrField{FieldSelector: Field(name)}
}

func (f StrField) Eq(v string) Matcher {
	return EqStr(f.FieldSelector, v)
}

func (f StrField) Gt(v string) Matcher {
	return GtStr(f.FieldSelector, v)
}

func (f StrField) Gte(v string) Matcher {
	return GteStr(f.FieldSelector, v)
}

func (f StrField) Lt(v string) Matcher {
	return LtStr(f.FieldSelector, v)
}

func (f StrField) Lte(v string) Matcher {
	return LteStr(f.FieldSelector, v)
}

type Table string

func (t Table) Name() string {
	return string(t)
}

func (t Table) SelectTable(tx *genji.Tx) (table.Table, error) {
	return tx.Table(string(t))
}
