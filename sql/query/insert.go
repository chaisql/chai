package query

import (
	"errors"
	"fmt"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
)

// InsertStmt is a DSL that allows creating a full Insert query.
type InsertStmt struct {
	TableName  string
	FieldNames []string
	Values     expr.LiteralExprList
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt InsertStmt) IsReadOnly() bool {
	return false
}

// Run the Insert statement in the given transaction.
// It implements the Statement interface.
func (stmt InsertStmt) Run(tx *database.Transaction, args []expr.Param) (Result, error) {
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

	stack := expr.EvalStack{
		Tx:     tx,
		Params: args,
	}

	if len(stmt.FieldNames) > 0 {
		return stmt.insertExprList(t, stack)
	}

	return stmt.insertDocuments(t, stack)
}

func (stmt InsertStmt) insertDocuments(t *database.Table, stack expr.EvalStack) (Result, error) {
	var res Result

	for _, e := range stmt.Values {
		v, err := e.Eval(stack)
		if err != nil {
			return res, err
		}

		if v.Type != document.DocumentValue {
			return res, fmt.Errorf("expected document, got %s", v.Type)
		}

		res.LastInsertKey, err = t.Insert(v.V.(document.Document))
		if err != nil {
			return res, err
		}

		res.RowsAffected++
	}

	return res, nil
}

func (stmt InsertStmt) insertExprList(t *database.Table, stack expr.EvalStack) (Result, error) {
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
			return res, fmt.Errorf("expected array, got %s", v.Type)
		}

		// iterate over each value
		v.V.(document.Array).Iterate(func(i int, v document.Value) error {
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
