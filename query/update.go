package query

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/asdine/genji"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

// UpdateStmt is a DSL that allows creating a full Update query.
// It is typically created using the Update function.
type UpdateStmt struct {
	tableSelector TableSelector
	pairs         map[string]Expr
	whereExpr     Expr
}

// Update creates a DSL equivalent to the SQL Update command.
func Update(tableSelector TableSelector) UpdateStmt {
	return UpdateStmt{
		tableSelector: tableSelector,
		pairs:         make(map[string]Expr),
	}
}

// Run the Update statement in a read-write transaction.
// It implements the Statement interface.
func (stmt UpdateStmt) Run(txm *TxOpener, args []driver.NamedValue) (res Result) {
	err := txm.Update(func(tx *genji.Tx) error {
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

// Exec the Update statement within tx.
func (stmt UpdateStmt) Exec(tx *genji.Tx, args ...interface{}) Result {
	nv := make([]driver.NamedValue, len(args))
	for i := range args {
		nv[i].Ordinal = i + 1
		nv[i].Value = args[i]
	}

	return stmt.exec(tx, nv)
}

// Exec the Update query within tx.
// If Where was called, records will be filtered depending on the result of the
// given expression. If the Where expression implements the IndexMatcher interface,
// the MatchIndex method will be called instead of the Eval one.
func (stmt UpdateStmt) exec(tx *genji.Tx, args []driver.NamedValue) Result {
	if stmt.tableSelector == nil {
		return Result{err: errors.New("missing table selector")}
	}

	if len(stmt.pairs) == 0 {
		return Result{err: errors.New("Set method not called")}
	}

	t, err := stmt.tableSelector.SelectTable(tx)
	if err != nil {
		return Result{err: err}
	}

	var tr table.Reader = t

	st := table.NewStream(tr)
	st = st.Filter(whereClause(tx, stmt.whereExpr))

	err = st.Iterate(func(recordID []byte, r record.Record) error {
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

			v, err := e.Eval(EvalContext{
				Tx:     tx,
				Record: r,
			})
			if err != nil {
				return err
			}

			lv, ok := v.(LitteralValue)
			if !ok {
				return fmt.Errorf("expected value got list")
			}

			f.Type = lv.Type
			f.Data = lv.Data
			err = fb.Replace(f.Name, f)
			if err != nil {
				return err
			}

			err = t.Replace(recordID, &fb)
			if err != nil {
				return err
			}
		}

		return nil
	})
	return Result{err: err}
}

// Set assignes the result of the evaluation of e into the field selected
// by f.
func (stmt UpdateStmt) Set(fieldName string, e Expr) UpdateStmt {
	stmt.pairs[fieldName] = e
	return stmt
}

// Where uses e to filter records if it evaluates to a falsy value.
// Calling this method is optional.
func (stmt UpdateStmt) Where(e Expr) UpdateStmt {
	stmt.whereExpr = e
	return stmt
}
