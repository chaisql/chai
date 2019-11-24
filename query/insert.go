package query

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/value"
)

// insertStmt is a DSL that allows creating a full Insert query.
type insertStmt struct {
	tableName  string
	fieldNames []string
	values     litteralExprList
	records    []interface{}
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt insertStmt) IsReadOnly() bool {
	return false
}

type kvPair struct {
	K string
	V expr
}

func (stmt insertStmt) Run(tx *database.Transaction, args []driver.NamedValue) (Result, error) {
	var res Result

	if stmt.tableName == "" {
		return res, errors.New("missing table name")
	}

	if stmt.values == nil && stmt.records == nil {
		return res, errors.New("values and records are empty")
	}

	t, err := tx.GetTable(stmt.tableName)
	if err != nil {
		return res, err
	}

	stack := evalStack{
		Tx:     tx,
		Params: args,
	}

	if len(stmt.records) > 0 {
		return stmt.insertRecords(t, stack)
	}

	return stmt.insertValues(t, stack)
}

type paramExtractor interface {
	Extract(params []driver.NamedValue) (interface{}, error)
}

func (stmt insertStmt) insertRecords(t *database.Table, stack evalStack) (Result, error) {
	var res Result
	var err error

	if len(stmt.fieldNames) > 0 {
		return res, errors.New("can't provide a field list with RECORDS clause")
	}

	for _, rec := range stmt.records {
		var r record.Record

		switch tp := rec.(type) {
		case record.Record:
			r = tp
		case paramExtractor:
			v, err := tp.Extract(stack.Params)
			if err != nil {
				return res, err
			}

			var ok bool
			r, ok = v.(record.Record)
			if !ok {
				return res, fmt.Errorf("unsupported parameter of type %t, expecting record.Record", v)
			}
		case []kvPair:
			var fb record.FieldBuffer
			for _, pair := range tp {
				v, err := pair.V.Eval(stack)
				if err != nil {
					return res, err
				}

				if v.IsList {
					return res, errors.New("invalid values")
				}

				fb.Add(record.Field{Name: pair.K, Value: v.Value.Value})
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

func (stmt insertStmt) insertValues(t *database.Table, stack evalStack) (Result, error) {
	var res Result

	// iterate over all of the records (r1, r2, r3, ...)
	for _, e := range stmt.values {
		var fb record.FieldBuffer

		v, err := e.Eval(stack)
		if err != nil {
			return res, err
		}

		// each record must be a list of values
		// (e1, e2, e3, ...)
		if !v.IsList {
			return res, errors.New("invalid values")
		}

		if len(stmt.fieldNames) != len(v.List) {
			return res, fmt.Errorf("%d values for %d fields", len(v.List), len(stmt.fieldNames))
		}

		// iterate over each value
		for i, v := range v.List {
			// get the field name
			fieldName := stmt.fieldNames[i]

			var lv *litteralValue

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
			fb.Add(record.Field{
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
