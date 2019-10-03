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
	tableName string
	whereExpr expr.Expr
}

// Delete creates a DSL equivalent to the SQL Delete command.
func Delete() DeleteStmt {
	return DeleteStmt{}
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt DeleteStmt) IsReadOnly() bool {
	return false
}

// Run runs the Delete statement in the given transaction.
// It implements the Statement interface.
func (stmt DeleteStmt) Run(tx *database.Tx, args []driver.NamedValue) Result {
	return stmt.exec(tx, args)
}

// Exec the Delete statement within tx.
func (stmt DeleteStmt) Exec(tx *database.Tx, args ...interface{}) Result {
	return stmt.exec(tx, argsToNamedValues(args))
}

// From indicates which table to select from.
// Calling this method before Run is mandatory.
func (stmt DeleteStmt) From(tableName string) DeleteStmt {
	stmt.tableName = tableName
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
	if stmt.tableName == "" {
		return Result{err: errors.New("missing table name")}
	}

	stack := expr.EvalStack{Tx: tx, Params: args}

	t, err := tx.GetTable(stmt.tableName)
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
