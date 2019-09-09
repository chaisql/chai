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

// Run the Delete query within tx.
// If Where was called, records will be filtered depending on the result of the
// given expression. If the Where expression implements the IndexMatcher interface,
// the MatchIndex method will be called instead of the Eval one.
func (d DeleteStmt) Run(tx *genji.Tx) error {
	if d.tableSelector == nil {
		return errors.New("missing table selector")
	}

	t, err := d.tableSelector.SelectTable(tx)
	if err != nil {
		return err
	}

	var useIndex bool
	var tr table.Reader = t

	if im, ok := d.whereExpr.(IndexMatcher); ok {
		tree, ok, err := im.MatchIndex(t)
		if err != nil && err != genji.ErrIndexNotFound {
			return err
		}

		if ok && err == nil {
			useIndex = true
			tr = &indexResultTable{
				tree:  tree,
				table: t,
			}
		}
	}

	st := table.NewStream(tr)

	if !useIndex {
		st = st.Filter(whereClause(tx, d.whereExpr))
	}

	return st.Iterate(func(recordID []byte, r record.Record) error {
		return t.Delete(recordID)
	})
}
