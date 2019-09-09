package query

import (
	"errors"
	"fmt"

	"github.com/asdine/genji"
	"github.com/asdine/genji/field"
	"github.com/asdine/genji/table"
)

// SelectStmt is a DSL that allows creating a full Select query.
// It is typically created using the Select function.
type SelectStmt struct {
	tableSelector TableSelector
	whereExpr     Expr
	offsetExpr    Expr
	limitExpr     Expr
}

// Select creates a DSL equivalent to the SQL Select command.
func Select() SelectStmt {
	return SelectStmt{}
}

// Run the Select query within tx.
// If Where was called, records will be filtered depending on the result of the
// given expression. If the Where expression implements the IndexMatcher interface,
// the MatchIndex method will be called instead of the Eval one.
func (q SelectStmt) Run(tx *genji.Tx) Result {
	if q.tableSelector == nil {
		return Result{err: errors.New("missing table selector")}
	}

	offset := -1
	limit := -1

	if q.offsetExpr != nil {
		s, err := q.offsetExpr.Eval(EvalContext{
			Tx: tx,
		})
		if err != nil {
			return Result{err: err}
		}
		if s.Type < field.Int {
			return Result{err: fmt.Errorf("offset expression must evaluate to a 64 bit integer, got %q", s.Type)}
		}
		offset, err = field.DecodeInt(s.Data)
		if err != nil {
			return Result{err: err}
		}
	}

	if q.limitExpr != nil {
		s, err := q.limitExpr.Eval(EvalContext{
			Tx: tx,
		})
		if err != nil {
			return Result{err: err}
		}
		if s.Type < field.Int {
			return Result{err: fmt.Errorf("limit expression must evaluate to a 64 bit integer, got %q", s.Type)}
		}
		limit, err = field.DecodeInt(s.Data)
		if err != nil {
			return Result{err: err}
		}
	}

	t, err := q.tableSelector.SelectTable(tx)
	if err != nil {
		return Result{err: err}
	}

	var tr table.Reader = t

	var useIndex bool
	if im, ok := q.whereExpr.(IndexMatcher); ok {
		tree, ok, err := im.MatchIndex(t)
		if err != nil && err != genji.ErrIndexNotFound {
			return Result{err: err}
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
		st = st.Filter(whereClause(tx, q.whereExpr))
	}

	if offset > 0 {
		st = st.Offset(offset)
	}

	if limit >= 0 {
		st = st.Limit(limit)
	}

	return Result{Stream: &st}
}

// Where uses e to filter records if it evaluates to a falsy value.
func (q SelectStmt) Where(e Expr) SelectStmt {
	q.whereExpr = e
	return q
}

// From indicates which table to select from.
// Calling this method before Run is mandatory.
func (q SelectStmt) From(tableSelector TableSelector) SelectStmt {
	q.tableSelector = tableSelector
	return q
}

// Limit the number of records returned.
func (q SelectStmt) Limit(offset int) SelectStmt {
	q.limitExpr = Int64Value(int64(offset))
	return q
}

// LimitExpr takes an expression that will be evaluated to determine
// how many records the query must return.
// The result of the evaluation must be an integer.
func (q SelectStmt) LimitExpr(e Expr) SelectStmt {
	q.limitExpr = e
	return q
}

// Offset indicates the number of records to skip.
func (q SelectStmt) Offset(offset int) SelectStmt {
	q.offsetExpr = Int64Value(int64(offset))
	return q
}

// OffsetExpr takes an expression that will be evaluated to determine
// how many records the query must skip.
// The result of the evaluation must be a field.Int64.
func (q SelectStmt) OffsetExpr(e Expr) SelectStmt {
	q.offsetExpr = e
	return q
}
