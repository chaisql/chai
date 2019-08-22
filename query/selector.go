package query

import (
	"github.com/asdine/genji"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
)

// A FieldSelector can extract a field from a record.
type FieldSelector interface {
	// SelectField takes a field from a record.
	// If the field selector was created using the As method
	// it must replace the name of f by the alias.
	SelectField(record.Record) (f field.Field, err error)
	// Name of the field selector.
	Name() string
	// As creates an alias to a field.
	// The returned field selector selects the same field but a different name
	// when the SelectField is called.
	As(alias string) FieldSelector
}

// TableSelector can select a table from a transaction.
type TableSelector interface {
	// SelectTable selects a table by calling the Table method of the transaction.
	SelectTable(*genji.Tx) (*genji.Table, error)
	// Name of the selected table.
	TableName() string
}

// A Field is an adapter that can turn a string into a field selector.
// It is supposed to be used by casting a string into a Field.
//   f := Field("Name")
//   f.SelectField(r)
// It implements the FieldSelector interface.
type Field string

// Name returns f as a string.
func (f Field) Name() string {
	return string(f)
}

// SelectField selects the field f from r.
func (f Field) SelectField(r record.Record) (field.Field, error) {
	return r.GetField(string(f))
}

// As returns a alias to f.
// The alias selects the same field as f but returns a different name
// when the SelectField method is called.
func (f Field) As(alias string) FieldSelector {
	return &Alias{FieldSelector: f, Alias: alias}
}

// An Alias is a field selector that wraps another one.
// If deleguates the field selection to the underlying field selector
// and replaces the Name attribute of the returned field by the value
// of the Alias attribute.
// It implements the FieldSelector interface.
type Alias struct {
	FieldSelector
	Alias string
}

// Name calls the underlying FieldSelector Name method.
func (a Alias) Name() string {
	return a.FieldSelector.Name()
}

// SelectField calls the SelectField method of FieldSelector.
// It returns a new field with the same data and type as the returned one
// but sets its name as the Alias attribute.
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

// A Table is an adapter that can turn a string into a table selector.
// It is supposed to be used by casting a string into a Table.
//   t := Table("Name")
//   t.SelectTable(tx)
// It implements the TableSelector interface.
type Table string

// TableName returns t as a string.
func (t Table) TableName() string {
	return string(t)
}

// SelectTable selects the table t from tx.
func (t Table) SelectTable(tx *genji.Tx) (*genji.Table, error) {
	return tx.GetTable(string(t))
}
