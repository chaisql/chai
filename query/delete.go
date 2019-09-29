package query

import (
	"database/sql/driver"
	"errors"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/query/expr"
	"github.com/asdine/genji/record"
)

// DeleteStmt is a DSL that allows creating a full Delete query.
// It is typically created using the Delete function.
type DeleteStmt struct {
	tableSelector TableSelector
	whereExpr     expr.Expr
}

// Delete creates a DSL equivalent to the SQL Delete command.
func Delete() DeleteStmt {
	return DeleteStmt{}
}

// Run the Delete statement in a read-write transaction.
// It implements the Statement interface.
func (stmt DeleteStmt) Run(txm *TxOpener, args []driver.NamedValue) (res Result) {
	err := txm.Update(func(tx *database.Tx) error {
		res = stmt.exec(tx, args)
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

// Exec the Delete statement within tx.
func (stmt DeleteStmt) Exec(tx *database.Tx, args ...interface{}) Result {
	return stmt.exec(tx, argsToNamedValues(args))
}

// From indicates which table to select from.
// Calling this method before Run is mandatory.
func (stmt DeleteStmt) From(tableSelector TableSelector) DeleteStmt {
	stmt.tableSelector = tableSelector
	return stmt
}

// Where uses e to filter records if it evaluates to a falsy value.
// Calling this method is optional.
func (stmt DeleteStmt) Where(e expr.Expr) DeleteStmt {
	stmt.whereExpr = e
	return stmt
}

// exec the Delete query within tx.
// If Where was called, records will be filtered depending on the result of the
// given expression. If the Where expression implements the IndexMatcher interface,
// the MatchIndex method will be called instead of the Eval one.
func (stmt DeleteStmt) exec(tx *database.Tx, args []driver.NamedValue) Result {
	if stmt.tableSelector == nil {
		return Result{err: errors.New("missing table selector")}
	}

	stack := expr.EvalStack{Tx: tx, Params: args}

	t, err := stmt.tableSelector.SelectTable(tx)
	if err != nil {
		return Result{err: err}
	}

	st := record.NewStream(t)
	st = st.Filter(whereClause(stmt.whereExpr, stack))

	err = st.Iterate(func(r record.Record) error {
		if k, ok := r.(record.Keyer); ok {
			return t.Delete(k.Key())
		}

		return errors.New("attempt to delete record without key")
	})
	return Result{err: err}
}
