package query

import (
	"errors"
	"fmt"

	"github.com/asdine/genji"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/value"
)

// InsertStmt is a DSL that allows creating a full Insert query.
// It is typically created using the Insert function.
type InsertStmt struct {
	tableSelector TableSelector
	fieldNames    []string
	values        LitteralExprList
}

// Insert creates a DSL equivalent to the SQL Insert command.
func Insert() InsertStmt {
	return InsertStmt{}
}

// Run runs the Insert statement in a read-write transaction.
// It implements the Statement interface.
func (stmt InsertStmt) Run(txm *TxOpener) (res Result) {
	err := txm.Update(func(tx *genji.Tx) error {
		res = stmt.Exec(tx)
		return nil
	})

	if res.err != nil {
		return
	}

	if err != nil {
		res.err = err
	}

	return
}

// Into indicates in which table to write the new records.
// Calling this method before Run is mandatory.
func (stmt InsertStmt) Into(tableSelector TableSelector) InsertStmt {
	stmt.tableSelector = tableSelector
	return stmt
}

// Fields to associate with values passed to the Values method.
func (stmt InsertStmt) Fields(fieldNames ...string) InsertStmt {
	stmt.fieldNames = append(stmt.fieldNames, fieldNames...)
	return stmt
}

// Values is called to add one record. The list of supplied values will be used as the fields
// of this record.
func (stmt InsertStmt) Values(values ...Expr) InsertStmt {
	stmt.values = append(stmt.values, LitteralExprList(values))
	return stmt
}

// Exec the Insert query within tx.
// If the Fields method was called prior to the Run method, each value will be associated with one of the given field name, in order.
// If the Fields method wasn't called, this will return an error.
func (stmt InsertStmt) Exec(tx *genji.Tx) Result {
	if stmt.tableSelector == nil {
		return Result{err: errors.New("missing table selector")}
	}

	if stmt.values == nil {
		return Result{err: errors.New("empty values")}
	}

	t, err := stmt.tableSelector.SelectTable(tx)
	if err != nil {
		return Result{err: err}
	}

	ectx := EvalContext{
		Tx: tx,
	}

	// iterate over all of the records (r1, r2, r3, ...)
	for _, e := range stmt.values {
		var fb record.FieldBuffer

		v, err := e.Eval(ectx)
		if err != nil {
			return Result{err: err}
		}

		// each record must be a list of expressions
		// (e1, e2, e3, ...)
		el, ok := v.(ExprList)
		if !ok {
			return Result{err: errors.New("invalid values")}
		}

		if len(stmt.fieldNames) != el.Length() {
			return Result{err: fmt.Errorf("%d values for %d fields", len(stmt.values), len(stmt.fieldNames))}
		}

		i := 0

		// iterate over each expression
		el.Iterate(func(e Expr) error {
			// get the field name
			fieldName := stmt.fieldNames[i]

			// evaluate the expression
			v, err := e.Eval(ectx)
			if err != nil {
				return err
			}

			// if the value is a list of expressions, evaluate recursively until
			// the result returns a simple value.
			if el, ok := v.(ExprList); ok {
				v, err = ValueFromExprList(ectx, el)
				if err != nil {
					return err
				}
			}

			// Assign the value to the field and add it to the record
			switch t := v.(type) {
			case LitteralValue:
				fb.Add(field.Field{
					Name: fieldName,
					Value: value.Value{
						Type: t.Type,
						Data: t.Data,
					},
				})
			case FieldExpr:
				fb.Add(field.Field{
					Name: fieldName,
					Value: value.Value{
						Type: t.Type,
						Data: t.Data,
					},
				})
			default:
				return fmt.Errorf("unsupported expression type %v", v)
			}

			i++
			return nil
		})

		_, err = t.Insert(&fb)
		if err != nil {
			return Result{err: err}
		}
	}

	return Result{}
}
