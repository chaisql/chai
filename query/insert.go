package query

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/asdine/genji"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
	"github.com/asdine/genji/value"
)

// InsertStmt is a DSL that allows creating a full Insert query.
// It is typically created using the Insert function.
type InsertStmt struct {
	tableSelector TableSelector
	fieldNames    []string
	values        LitteralExprList
	records       []record.Record
	pairsList     [][]kvPair
}

// Insert creates a DSL equivalent to the SQL Insert command.
func Insert() InsertStmt {
	return InsertStmt{}
}

// Run runs the Insert statement in a read-write transaction.
// It implements the Statement interface.
func (stmt InsertStmt) Run(txm *TxOpener, args []driver.NamedValue) (res Result) {
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

// Exec the Insert query within tx.
func (stmt InsertStmt) Exec(tx *genji.Tx, args ...interface{}) Result {
	return stmt.exec(tx, argsToNamedValues(args))
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

// Records is called to add one or more records.
func (stmt InsertStmt) Records(records ...record.Record) InsertStmt {
	stmt.records = append(stmt.records, records...)
	return stmt
}

func (stmt InsertStmt) pairs(pairs ...kvPair) InsertStmt {
	stmt.pairsList = append(stmt.pairsList, pairs)
	return stmt
}

func (stmt InsertStmt) exec(tx *genji.Tx, args []driver.NamedValue) Result {
	if stmt.tableSelector == nil {
		return Result{err: errors.New("missing table selector")}
	}

	if stmt.values == nil && stmt.records == nil && stmt.pairsList == nil {
		return Result{err: errors.New("values and records are empty")}
	}

	t, err := stmt.tableSelector.SelectTable(tx)
	if err != nil {
		return Result{err: err}
	}

	ectx := EvalContext{
		Tx: tx,
	}

	if len(stmt.pairsList) > 0 {
		return stmt.insertPairList(t, ectx)
	}

	if len(stmt.records) > 0 {
		return stmt.insertRecords(t, ectx)
	}

	return stmt.insertValues(t, ectx)
}

func (stmt InsertStmt) insertPairList(t *genji.Table, ectx EvalContext) Result {
	if len(stmt.fieldNames) > 0 {
		return Result{err: errors.New("can't provide a field list with RECORDS clause")}
	}

	var res Result
	var err error

	for _, pairs := range stmt.pairsList {
		var fb record.FieldBuffer
		for _, pair := range pairs {
			v, err := pair.e.Eval(ectx)
			if err != nil {
				res.err = err
				return res
			}

			vl, ok := v.(LitteralValue)
			if !ok {
				res.err = errors.New("invalid values")
				return res
			}

			fb.Add(field.Field{Name: pair.k, Value: vl.Value})
		}

		res.lastInsertRecordID, err = t.Insert(&fb)
		if err != nil {
			res.err = err
			return res
		}

		res.rowsAffected++
	}

	res.Stream = table.NewStream(table.NewReaderFromRecords())
	return res
}

func (stmt InsertStmt) insertRecords(t *genji.Table, ectx EvalContext) Result {
	var res Result
	var err error

	for _, rec := range stmt.records {
		res.lastInsertRecordID, err = t.Insert(rec)
		if err != nil {
			return Result{err: err}
		}

		res.rowsAffected++
	}

	res.Stream = table.NewStream(table.NewReaderFromRecords())
	return res
}

func (stmt InsertStmt) insertValues(t *genji.Table, ectx EvalContext) Result {
	var res Result

	// iterate over all of the records (r1, r2, r3, ...)
	for _, e := range stmt.values {
		var fb record.FieldBuffer

		v, err := e.Eval(ectx)
		if err != nil {
			return Result{err: err}
		}

		// each record must be a list of values
		// (e1, e2, e3, ...)
		vl, ok := v.(LitteralValueList)
		if !ok {
			return Result{err: errors.New("invalid values")}
		}

		if len(stmt.fieldNames) != len(vl) {
			return Result{err: fmt.Errorf("%d values for %d fields", len(vl), len(stmt.fieldNames))}
		}

		// iterate over each value
		for i, v := range vl {
			// get the field name
			fieldName := stmt.fieldNames[i]

			var lv *LitteralValue

			// each value must be either a LitteralValue or a LitteralValueList with exactly
			// one value
			switch t := v.(type) {
			case LitteralValue:
				lv = &t
			case LitteralValueList:
				if len(t) == 1 {
					if val, ok := t[0].(LitteralValue); ok {
						lv = &val
					}
				}
				return Result{err: fmt.Errorf("value expected, got list")}
			}

			// Assign the value to the field and add it to the record
			fb.Add(field.Field{
				Name: fieldName,
				Value: value.Value{
					Type: lv.Type,
					Data: lv.Data,
				},
			})
		}

		res.lastInsertRecordID, err = t.Insert(&fb)
		if err != nil {
			return Result{err: err}
		}

		res.rowsAffected++
	}

	res.Stream = table.NewStream(table.NewReaderFromRecords())

	return res
}
