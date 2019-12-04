package query

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/document"
)

// InsertStmt is a DSL that allows creating a full Insert query.
type InsertStmt struct {
	TableName  string
	FieldNames []string
	Values     LiteralExprList
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt InsertStmt) IsReadOnly() bool {
	return false
}

func (stmt InsertStmt) Run(tx *database.Transaction, args []driver.NamedValue) (Result, error) {
	var res Result

	if stmt.TableName == "" {
		return res, errors.New("missing table name")
	}

	if stmt.Values == nil {
		return res, errors.New("values are empty")
	}

	t, err := tx.GetTable(stmt.TableName)
	if err != nil {
		return res, err
	}

	stack := EvalStack{
		Tx:     tx,
		Params: args,
	}

	if len(stmt.FieldNames) > 0 {
		return stmt.insertExprList(t, stack)
	}

	return stmt.insertDocuments(t, stack)
}

type paramExtractor interface {
	Extract(params []driver.NamedValue) (interface{}, error)
}

func (stmt InsertStmt) insertDocuments(t *database.Table, stack EvalStack) (Result, error) {
	var res Result
	var err error

	for _, rec := range stmt.Values {
		var d document.Document

		switch tp := rec.(type) {
		case document.Document:
			d = tp
		case paramExtractor:
			v, err := tp.Extract(stack.Params)
			if err != nil {
				return res, err
			}

			var ok bool
			d, ok = v.(document.Document)
			if !ok {
				return res, fmt.Errorf("unsupported parameter of type %t, expecting document.Document", v)
			}
		case LiteralValue:
			if tp.Value.Type != document.DocumentValue {
				return res, fmt.Errorf("values must be a list of documents if field list is empty")
			}

			d, err = tp.Value.DecodeToDocument()
			if err != nil {
				return res, err
			}
		case KVPairs:
			v, err := tp.Eval(stack)
			if err != nil {
				return res, err
			}
			d, err = v.Value.Value.DecodeToDocument()
			if err != nil {
				return res, err
			}
		default:
			return res, fmt.Errorf("values must be a list of documents if field list is empty")
		}

		res.lastInsertKey, err = t.Insert(d)
		if err != nil {
			return res, err
		}

		res.rowsAffected++
	}

	return res, nil
}

func (stmt InsertStmt) insertExprList(t *database.Table, stack EvalStack) (Result, error) {
	var res Result

	// iterate over all of the documents (r1, r2, r3, ...)
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
			fb.Add(fieldName, document.Value{
				Type: lv.Type,
				Data: lv.Data,
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
