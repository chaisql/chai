package query

import (
	"errors"
	"fmt"

	"github.com/asdine/genji"
	"github.com/asdine/genji/table"
	"github.com/asdine/genji/value"
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

// Run the Select statement in a read-only transaction.
// It implements the Statement interface.
func (s SelectStmt) Run(txm *TxOpener) (res Result) {
	err := txm.View(func(tx *genji.Tx) error {
		res = s.Exec(tx)
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

// Exec the Select query within tx.
// If Where was called, records will be filtered depending on the result of the
// given expression. If the Where expression implements the IndexMatcher interface,
// the MatchIndex method will be called instead of the Eval one.
func (s SelectStmt) Exec(tx *genji.Tx) Result {
	if s.tableSelector == nil {
		return Result{err: errors.New("missing table selector")}
	}

	offset := -1
	limit := -1

	if s.offsetExpr != nil {
		v, err := s.offsetExpr.Eval(EvalContext{
			Tx: tx,
		})
		if err != nil {
			return Result{err: err}
		}

		lv, ok := v.(LitteralValue)
		if !ok {
			return Result{err: fmt.Errorf("expected value got list")}
		}

		if lv.Type < value.Int {
			return Result{err: fmt.Errorf("offset expression must evaluate to a 64 bit integer, got %q", lv.Type)}
		}

		offset, err = value.DecodeInt(lv.Data)
		if err != nil {
			return Result{err: err}
		}
	}

	if s.limitExpr != nil {
		v, err := s.limitExpr.Eval(EvalContext{
			Tx: tx,
		})
		if err != nil {
			return Result{err: err}
		}

		lv, ok := v.(LitteralValue)
		if !ok {
			return Result{err: fmt.Errorf("expected value got list")}
		}

		if lv.Type < value.Int {
			return Result{err: fmt.Errorf("limit expression must evaluate to a 64 bit integer, got %q", lv.Type)}
		}

		limit, err = value.DecodeInt(lv.Data)
		if err != nil {
			return Result{err: err}
		}
	}

	t, err := s.tableSelector.SelectTable(tx)
	if err != nil {
		return Result{err: err}
	}

	var tr table.Reader = t

	st := table.NewStream(tr)
	st = st.Filter(whereClause(tx, s.whereExpr))

	if offset > 0 {
		st = st.Offset(offset)
	}

	if limit >= 0 {
		st = st.Limit(limit)
	}

	return Result{Stream: &st}
}

// Where uses e to filter records if it evaluates to a falsy value.
func (s SelectStmt) Where(e Expr) SelectStmt {
	s.whereExpr = e
	return s
}

// From indicates which table to select from.
// Calling this method before Run is mandatory.
func (s SelectStmt) From(tableSelector TableSelector) SelectStmt {
	s.tableSelector = tableSelector
	return s
}

// Limit the number of records returned.
func (s SelectStmt) Limit(offset int) SelectStmt {
	s.limitExpr = Int64Value(int64(offset))
	return s
}

// LimitExpr takes an expression that will be evaluated to determine
// how many records the query must return.
// The result of the evaluation must be an integer.
func (s SelectStmt) LimitExpr(e Expr) SelectStmt {
	s.limitExpr = e
	return s
}

// Offset indicates the number of records to skip.
func (s SelectStmt) Offset(offset int) SelectStmt {
	s.offsetExpr = Int64Value(int64(offset))
	return s
}

// OffsetExpr takes an expression that will be evaluated to determine
// how many records the query must skip.
// The result of the evaluation must be a field.Int64.
func (s SelectStmt) OffsetExpr(e Expr) SelectStmt {
	s.offsetExpr = e
	return s
}
