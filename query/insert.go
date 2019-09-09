package query

import (
	"errors"
	"fmt"

	"github.com/asdine/genji"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

// InsertStmt is a DSL that allows creating a full Insert query.
// It is typically created using the Insert function.
type InsertStmt struct {
	tableSelector TableSelector
	fieldNames    []string
	values        []Expr
}

// Insert creates a DSL equivalent to the SQL Insert command.
func Insert() InsertStmt {
	return InsertStmt{}
}

// Exec runs the Insert statement in a read-write transaction.
// It implements the Statement interface.
func (i InsertStmt) Exec(txm *TxOpener) (res Result) {
	err := txm.Update(func(tx *genji.Tx) error {
		res = i.Run(tx)
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
func (i InsertStmt) Into(tableSelector TableSelector) InsertStmt {
	i.tableSelector = tableSelector
	return i
}

// Fields to associate with values passed to the Values method.
func (i InsertStmt) Fields(fieldNames ...string) InsertStmt {
	i.fieldNames = append(i.fieldNames, fieldNames...)
	return i
}

// Values to associate with the record fields.
func (i InsertStmt) Values(values ...Expr) InsertStmt {
	i.values = append(i.values, values...)
	return i
}

// Run the Insert query within tx.
// If the Fields method was called prior to the Run method, each value will be associated with one of the given field name, in order.
// If the Fields method wasn't called, this will return an error
func (i InsertStmt) Run(tx *genji.Tx) Result {
	if i.tableSelector == nil {
		return Result{err: errors.New("missing table selector")}
	}

	if i.values == nil {
		return Result{err: errors.New("empty values")}
	}

	t, err := i.tableSelector.SelectTable(tx)
	if err != nil {
		return Result{err: err}
	}

	var fb record.FieldBuffer

	if len(i.fieldNames) != len(i.values) {
		return Result{err: fmt.Errorf("%d values for %d fields", len(i.values), len(i.fieldNames))}
	}

	for idx, name := range i.fieldNames {
		sc, err := i.values[idx].Eval(EvalContext{
			Tx: tx,
		})
		if err != nil {
			return Result{err: err}
		}

		fb.Add(field.Field{
			Name: name,
			Type: sc.Type,
			Data: sc.Data,
		})
	}

	recordID, err := t.Insert(&fb)
	if err != nil {
		return Result{err: err}
	}

	st := table.NewStream(table.NewReaderFromRecords(record.FieldBuffer([]field.Field{
		field.NewBytes("recordID", recordID),
	})))
	return Result{Stream: &st}
}
