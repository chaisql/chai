package query

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/query/expr"
	"github.com/asdine/genji/record"
)

// UpdateStmt is a DSL that allows creating a full Update query.
// It is typically created using the Update function.
type UpdateStmt struct {
	tableName string
	pairs     map[string]expr.Expr
	whereExpr expr.Expr
}

// Update creates a DSL equivalent to the SQL Update command.
func Update(tableName string) UpdateStmt {
	return UpdateStmt{
		tableName: tableName,
		pairs:     make(map[string]expr.Expr),
	}
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt UpdateStmt) IsReadOnly() bool {
	return false
}

// Run runs the Update table statement in the given transaction.
// It implements the Statement interface.
func (stmt UpdateStmt) Run(tx *database.Tx, args []driver.NamedValue) Result {
	return stmt.exec(tx, args)
}

// Exec the Update statement within tx.
func (stmt UpdateStmt) Exec(tx *database.Tx, args ...interface{}) Result {
	return stmt.exec(tx, argsToNamedValues(args))
}

// Set assignes the result of the evaluation of e into the field selected
// by f.
func (stmt UpdateStmt) Set(fieldName string, e expr.Expr) UpdateStmt {
	stmt.pairs[fieldName] = e
	return stmt
}

// Where uses e to filter records if it evaluates to a falsy value.
// Calling this method is optional.
func (stmt UpdateStmt) Where(e expr.Expr) UpdateStmt {
	stmt.whereExpr = e
	return stmt
}

// Exec the Update query within tx.
// If Where was called, records will be filtered depending on the result of the
// given expression. If the Where expression implements the IndexMatcher interface,
// the MatchIndex method will be called instead of the Eval one.
func (stmt UpdateStmt) exec(tx *database.Tx, args []driver.NamedValue) Result {
	if stmt.tableName == "" {
		return Result{err: errors.New("missing table name")}
	}

	if len(stmt.pairs) == 0 {
		return Result{err: errors.New("Set method not called")}
	}

	stack := expr.EvalStack{
		Tx:     tx,
		Params: args,
	}

	t, err := tx.GetTable(stmt.tableName)
	if err != nil {
		return Result{err: err}
	}

	st := record.NewStream(t)
	st = st.Filter(whereClause(stmt.whereExpr, stack))

	err = st.Iterate(func(r record.Record) error {
		rk, ok := r.(record.Keyer)
		if !ok {
			return errors.New("attempt to update record without key")
		}

		var fb record.FieldBuffer
		err := fb.ScanRecord(r)
		if err != nil {
			return err
		}

		for fname, e := range stmt.pairs {
			f, err := fb.GetField(fname)
			if err != nil {
				return err
			}

			v, err := e.Eval(expr.EvalStack{
				Tx:     tx,
				Record: r,
			})
			if err != nil {
				return err
			}

			lv, ok := v.(expr.LitteralValue)
			if !ok {
				return fmt.Errorf("expected value got list")
			}

			f.Type = lv.Type
			f.Data = lv.Data
			err = fb.Replace(f.Name, f)
			if err != nil {
				return err
			}

			err = t.Replace(rk.Key(), &fb)
			if err != nil {
				return err
			}
		}

		return nil
	})
	return Result{err: err}
}
