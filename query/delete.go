package query

import (
	"errors"

	"github.com/asdine/genji"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/table"
)

// DeleteStmt is a DSL that allows creating a full Delete query.
// It is typically created using the Delete function.
type DeleteStmt struct {
	tableSelector TableSelector
	whereExpr     Expr
}

// Delete creates a DSL equivalent to the SQL Delete command.
func Delete() DeleteStmt {
	return DeleteStmt{}
}

// Run the Delete statement in a read-write transaction.
// It implements the Statement interface.
func (d DeleteStmt) Run(txm *TxOpener) (res Result) {
	err := txm.Update(func(tx *genji.Tx) error {
		res = d.Exec(tx)
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

// From indicates which table to select from.
// Calling this method before Run is mandatory.
func (d DeleteStmt) From(tableSelector TableSelector) DeleteStmt {
	d.tableSelector = tableSelector
	return d
}

// Where uses e to filter records if it evaluates to a falsy value.
// Calling this method is optional.
func (d DeleteStmt) Where(e Expr) DeleteStmt {
	d.whereExpr = e
	return d
}

// Exec the Delete query within tx.
// If Where was called, records will be filtered depending on the result of the
// given expression. If the Where expression implements the IndexMatcher interface,
// the MatchIndex method will be called instead of the Eval one.
func (d DeleteStmt) Exec(tx *genji.Tx) Result {
	if d.tableSelector == nil {
		return Result{err: errors.New("missing table selector")}
	}

	t, err := d.tableSelector.SelectTable(tx)
	if err != nil {
		return Result{err: err}
	}

	var tr table.Reader = t

	st := table.NewStream(tr)
	st = st.Filter(whereClause(tx, d.whereExpr))

	err = st.Iterate(func(recordID []byte, r record.Record) error {
		return t.Delete(recordID)
	})
	return Result{err: err}
}
