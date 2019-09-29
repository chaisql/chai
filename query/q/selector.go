package q

import (
	"github.com/asdine/genji/database"
	"github.com/asdine/genji/query"
	"github.com/asdine/genji/query/expr"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/value"
)

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
func (f Field) SelectField(r record.Record) (record.Field, error) {
	return r.GetField(string(f))
}

// Eval extracts the record from the context and selects the right field.
// It implements the Expr interface.
func (f Field) Eval(stack expr.EvalStack) (expr.Value, error) {
	fd, err := f.SelectField(stack.Record)
	if err != nil {
		return expr.NilLitteral, nil
	}

	return expr.LitteralValue{Value: fd.Value}, nil
}

// As returns a alias to f.
// The alias selects the same field as f but returns a different name
// when the SelectField method is called.
func (f Field) As(alias string) query.FieldSelector {
	return &Alias{FieldSelector: f, Alias: alias}
}

// An Alias is a field selector that wraps another one.
// If deleguates the field selection to the underlying field selector
// and replaces the Name attribute of the returned field by the value
// of the Alias attribute.
// It implements the FieldSelector interface.
type Alias struct {
	query.FieldSelector
	Alias string
}

// Name calls the underlying FieldSelector Name method.
func (a Alias) Name() string {
	return a.FieldSelector.Name()
}

// SelectField calls the SelectField method of FieldSelector.
// It returns a new field with the same data and type as the returned one
// but sets its name as the Alias attribute.
func (a Alias) SelectField(r record.Record) (record.Field, error) {
	f, err := a.FieldSelector.SelectField(r)
	if err != nil {
		return record.Field{}, err
	}

	return record.Field{
		Value: value.Value{
			Data: f.Data,
			Type: f.Type,
		},
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
func (t Table) SelectTable(tx *database.Tx) (*database.Table, error) {
	return tx.GetTable(string(t))
}
