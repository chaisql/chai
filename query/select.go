package query

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/query/expr"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/value"
)

// SelectStmt is a DSL that allows creating a full Select query.
// It is typically created using the Select function.
type SelectStmt struct {
	tableSelector  TableSelector
	whereExpr      expr.Expr
	offsetExpr     expr.Expr
	limitExpr      expr.Expr
	FieldSelectors []FieldSelector
}

// Select creates a DSL equivalent to the SQL Select command.
func Select(fields ...FieldSelector) SelectStmt {
	return SelectStmt{
		FieldSelectors: fields,
	}
}

// IsReadOnly always returns true. It implements the Statement interface.
func (stmt SelectStmt) IsReadOnly() bool {
	return true
}

// Run the Select statement in the given transaction.
// It implements the Statement interface.
func (stmt SelectStmt) Run(tx *database.Tx, args []driver.NamedValue) Result {
	return stmt.exec(tx, args)
}

// Exec the Select statement within tx.
func (stmt SelectStmt) Exec(tx *database.Tx, args ...interface{}) Result {
	return stmt.exec(tx, argsToNamedValues(args))
}

// Where uses e to filter records if it evaluates to a falsy value.
func (stmt SelectStmt) Where(e expr.Expr) SelectStmt {
	stmt.whereExpr = e
	return stmt
}

// From indicates which table to select from.
// Calling this method before Run is mandatory.
func (stmt SelectStmt) From(tableSelector TableSelector) SelectStmt {
	stmt.tableSelector = tableSelector
	return stmt
}

// Limit the number of records returned.
func (stmt SelectStmt) Limit(limit int) SelectStmt {
	stmt.limitExpr = expr.Int64Value(int64(limit))
	return stmt
}

// LimitExpr takes an expression that will be evaluated to determine
// how many records the query must return.
// The result of the evaluation must be an integer.
func (stmt SelectStmt) LimitExpr(e expr.Expr) SelectStmt {
	stmt.limitExpr = e
	return stmt
}

// Offset indicates the number of records to skip.
func (stmt SelectStmt) Offset(offset int) SelectStmt {
	stmt.offsetExpr = expr.Int64Value(int64(offset))
	return stmt
}

// OffsetExpr takes an expression that will be evaluated to determine
// how many records the query must skip.
// The result of the evaluation must be a field.Int64.
func (stmt SelectStmt) OffsetExpr(e expr.Expr) SelectStmt {
	stmt.offsetExpr = e
	return stmt
}

// Exec the Select query within tx.
// If Where was called, records will be filtered depending on the result of the
// given expression. If the Where expression implements the IndexMatcher interface,
// the MatchIndex method will be called instead of the Eval one.
func (stmt SelectStmt) exec(tx *database.Tx, args []driver.NamedValue) Result {
	if stmt.tableSelector == nil {
		return Result{err: errors.New("missing table selector")}
	}

	ts, err := newQueryOptimizer(tx, stmt.tableSelector).optimizeQuery(stmt.whereExpr, args)
	if err != nil {
		return Result{err: err}
	}

	offset := -1
	limit := -1

	stack := expr.EvalStack{
		Tx:     tx,
		Params: args,
	}

	if stmt.offsetExpr != nil {
		v, err := stmt.offsetExpr.Eval(stack)
		if err != nil {
			return Result{err: err}
		}

		if v.IsList {
			return Result{err: fmt.Errorf("expected value got list")}
		}

		if v.Value.Type < value.Int {
			return Result{err: fmt.Errorf("offset expression must evaluate to a 64 bit integer, got %q", v.Value.Type)}
		}

		offset, err = value.DecodeInt(v.Value.Data)
		if err != nil {
			return Result{err: err}
		}
	}

	if stmt.limitExpr != nil {
		v, err := stmt.limitExpr.Eval(stack)
		if err != nil {
			return Result{err: err}
		}

		if v.IsList {
			return Result{err: fmt.Errorf("expected value got list")}
		}

		if v.Value.Type < value.Int {
			return Result{err: fmt.Errorf("limit expression must evaluate to a 64 bit integer, got %q", v.Value.Type)}
		}

		limit, err = value.DecodeInt(v.Value.Data)
		if err != nil {
			return Result{err: err}
		}
	}

	t, err := ts.SelectTable(tx)
	if err != nil {
		return Result{err: err}
	}

	st := record.NewStream(t)
	st = st.Filter(whereClause(stmt.whereExpr, stack))

	if offset > 0 {
		st = st.Offset(offset)
	}

	if limit >= 0 {
		st = st.Limit(limit)
	}

	if len(stmt.FieldSelectors) > 0 {
		fieldNames := make([]string, len(stmt.FieldSelectors))
		for i := range stmt.FieldSelectors {
			fieldNames[i] = stmt.FieldSelectors[i].Name()
		}
		st = st.Map(func(r record.Record) (record.Record, error) {
			return recordMask{
				r:      r,
				fields: fieldNames,
			}, nil
		})
	}

	return Result{Stream: st}
}

type recordMask struct {
	r      record.Record
	fields []string
}

var _ record.Record = recordMask{}

func (r recordMask) GetField(name string) (record.Field, error) {
	for _, n := range r.fields {
		if n == name {
			return r.r.GetField(name)
		}
	}

	return record.Field{}, fmt.Errorf("field %q not found", name)
}

func (r recordMask) Iterate(fn func(f record.Field) error) error {
	for _, n := range r.fields {
		f, err := r.r.GetField(n)
		if err != nil {
			return err
		}

		err = fn(f)
		if err != nil {
			return err
		}
	}

	return nil
}
