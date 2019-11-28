package query

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/value"
)

// InsertStmt is a DSL that allows creating a full Insert query.
type InsertStmt struct {
	TableName  string
	FieldNames []string
	Values     LiteralExprList
	Records    []interface{}
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt InsertStmt) IsReadOnly() bool {
	return false
}

type KVPair struct {
	K string
	V Expr
}

func (stmt InsertStmt) Run(tx *database.Transaction, args []driver.NamedValue) (Result, error) {
	var res Result

	if stmt.TableName == "" {
		return res, errors.New("missing table name")
	}

	if stmt.Values == nil && stmt.Records == nil {
		return res, errors.New("values and records are empty")
	}

	t, err := tx.GetTable(stmt.TableName)
	if err != nil {
		return res, err
	}

	stack := EvalStack{
		Tx:     tx,
		Params: args,
	}

	if len(stmt.Records) > 0 {
		return stmt.insertRecords(t, stack)
	}

	return stmt.insertValues(t, stack)
}

type paramExtractor interface {
	Extract(params []driver.NamedValue) (interface{}, error)
}

func (stmt InsertStmt) insertRecords(t *database.Table, stack EvalStack) (Result, error) {
	var res Result
	var err error

	if len(stmt.FieldNames) > 0 {
		return res, errors.New("can't provide a field list with RECORDS clause")
	}

	for _, rec := range stmt.Records {
		var r document.Record

		switch tp := rec.(type) {
		case document.Record:
			r = tp
		case paramExtractor:
			v, err := tp.Extract(stack.Params)
			if err != nil {
				return res, err
			}

			var ok bool
			r, ok = v.(document.Record)
			if !ok {
				return res, fmt.Errorf("unsupported parameter of type %t, expecting document.Record", v)
			}
		case []KVPair:
			var fb document.FieldBuffer
			for _, pair := range tp {
				v, err := pair.V.Eval(stack)
				if err != nil {
					return res, err
				}

				if v.IsList {
					return res, errors.New("invalid values")
				}

				fb.Add(document.Field{Name: pair.K, Value: v.Value.Value})
			}
			r = &fb
		}

		res.lastInsertKey, err = t.Insert(r)
		if err != nil {
			return res, err
		}

		res.rowsAffected++
	}

	return res, nil
}

func (stmt InsertStmt) insertValues(t *database.Table, stack EvalStack) (Result, error) {
	var res Result

	// iterate over all of the records (r1, r2, r3, ...)
	for _, e := range stmt.Values {
		var fb document.FieldBuffer

		v, err := e.Eval(stack)
		if err != nil {
			return res, err
		}

		// each record must be a list of values
		// (e1, e2, e3, ...)
		if !v.IsList {
			return res, errors.New("invalid values")
		}

		if len(stmt.FieldNames) != len(v.List) {
			return res, fmt.Errorf("%d values for %d fields", len(v.List), len(stmt.FieldNames))
		}

		// iterate over each value
		for i, v := range v.List {
			// get the field name
			fieldName := stmt.FieldNames[i]

			var lv *LiteralValue

			// each value must be either a LitteralValue or a LitteralValueList with exactly
			// one value
			if !v.IsList {
				lv = &v.Value
			} else {
				if len(v.List) == 1 {
					if val := v.List[0]; !val.IsList {
						lv = &val.Value
					}
				}
				return res, fmt.Errorf("value expected, got list")
			}

			// Assign the value to the field and add it to the record
			fb.Add(document.Field{
				Name: fieldName,
				Value: value.Value{
					Type: lv.Type,
					Data: lv.Data,
				},
			})
		}

		res.lastInsertKey, err = t.Insert(&fb)
		if err != nil {
			return res, err
		}

		res.rowsAffected++
	}

	return res, nil
}
