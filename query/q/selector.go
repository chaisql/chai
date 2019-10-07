package q

import (
	"github.com/asdine/genji/database"
	"github.com/asdine/genji/query/expr"
	"github.com/asdine/genji/record"
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

	return expr.NewSingleValue(fd.Value), nil
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
func (t Table) SelectTable(tx *database.Tx) (record.Iterator, error) {
	return tx.GetTable(string(t))
}
