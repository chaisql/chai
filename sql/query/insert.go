package query

import (
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

// Run the Insert statement in the given transaction.
// It implements the Statement interface.
func (stmt InsertStmt) Run(tx *database.Transaction, args []Param) (Result, error) {
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
	extract(params []Param) (interface{}, error)
}

func (stmt InsertStmt) insertDocuments(t *database.Table, stack EvalStack) (Result, error) {
	var res Result
	var err error

	for _, doc := range stmt.Values {
		var d document.Document

		switch tp := doc.(type) {
		case document.Document:
			d = tp
		case paramExtractor:
			d, err = extractDocumentFromParamExtractor(tp, stack.Params)
			if err != nil {
				return res, err
			}
		case LiteralValue:
			v := document.Value(tp)

			if v.Type != document.DocumentValue {
				return res, fmt.Errorf("values must be a list of documents if field list is empty")
			}

			d, err = v.ConvertToDocument()
			if err != nil {
				return res, err
			}
		case KVPairs:
			v, err := tp.Eval(stack)
			if err != nil {
				return res, err
			}
			d, err = v.ConvertToDocument()
			if err != nil {
				return res, err
			}
		default:
			return res, fmt.Errorf("values must be a list of documents if field list is empty")
		}

		res.LastInsertKey, err = t.Insert(d)
		if err != nil {
			return res, err
		}

		res.RowsAffected++
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

		// each document must be a list of expressions
		// (e1, e2, e3, ...) or [e1, e2, e2, ....]
		if v.Type != document.ArrayValue {
			return res, errors.New("invalid values")
		}

		vlist, err := v.ConvertToArray()
		if err != nil {
			return res, err
		}

		lenv, err := document.ArrayLength(vlist)
		if err != nil {
			return res, err
		}

		if len(stmt.FieldNames) != lenv {
			return res, fmt.Errorf("%d values for %d fields", lenv, len(stmt.FieldNames))
		}

		// iterate over each value
		vlist.Iterate(func(i int, v document.Value) error {
			// get the field name
			fieldName := stmt.FieldNames[i]

			// Assign the value to the field and add it to the document
			fb.Add(fieldName, v)

			return nil
		})

		res.LastInsertKey, err = t.Insert(&fb)
		if err != nil {
			return res, err
		}

		res.RowsAffected++
	}

	return res, nil
}
