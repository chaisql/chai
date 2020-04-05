package query

import (
	"errors"
	"fmt"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/sql/scanner"
)

// SelectStmt is a DSL that allows creating a full Select query.
type SelectStmt struct {
	TableName        string
	WhereExpr        Expr
	OrderBy          FieldSelector
	OrderByDirection scanner.Token
	OffsetExpr       Expr
	LimitExpr        Expr
	Selectors        []ResultField
}

// IsReadOnly always returns true. It implements the Statement interface.
func (stmt SelectStmt) IsReadOnly() bool {
	return true
}

// Run the Select statement in the given transaction.
// It implements the Statement interface.
func (stmt SelectStmt) Run(tx *database.Transaction, args []Param) (Result, error) {
	return stmt.exec(tx, args)
}

// Exec the Select query within tx.
func (stmt SelectStmt) exec(tx *database.Transaction, args []Param) (Result, error) {
	var res Result

	// if there is no table name specified, evaluate the expression immediatly and return
	// a stream with the result.
	if stmt.TableName == "" {
		if len(stmt.Selectors) == 0 {
			return res, errors.New("missing table selector")
		}

		d := documentMask{
			resultFields: stmt.Selectors,
		}
		var fb document.FieldBuffer
		err := d.Iterate(func(f string, v document.Value) error {
			fb.Add(f, v)
			return nil
		})
		if err != nil {
			return Result{}, err
		}

		return Result{Stream: document.NewStream(document.NewIterator(fb))}, nil
	}

	if stmt.OrderByDirection != scanner.DESC {
		stmt.OrderByDirection = scanner.ASC
	}

	offset := -1
	limit := -1

	stack := EvalStack{
		Tx:     tx,
		Params: args,
	}

	if stmt.OffsetExpr != nil {
		v, err := stmt.OffsetExpr.Eval(stack)
		if err != nil {
			return res, err
		}

		if !v.Type.IsNumber() {
			return res, fmt.Errorf("offset expression must evaluate to a number, got %q", v.Type)
		}

		voff, err := v.ConvertToInt64()
		if err != nil {
			return res, err
		}
		offset = int(voff)
	}

	if stmt.LimitExpr != nil {
		v, err := stmt.LimitExpr.Eval(stack)
		if err != nil {
			return res, err
		}

		if !v.Type.IsNumber() {
			return res, fmt.Errorf("limit expression must evaluate to a number, got %q", v.Type)
		}

		vlim, err := v.ConvertToInt64()
		if err != nil {
			return res, err
		}
		limit = int(vlim)
	}

	qo, err := newQueryOptimizer(tx, stmt.TableName)
	if err != nil {
		return res, err
	}
	qo.whereExpr = stmt.WhereExpr
	qo.args = args
	qo.orderBy = stmt.OrderBy
	qo.orderByDirection = stmt.OrderByDirection
	qo.limit = limit
	qo.offset = offset

	st, err := qo.optimizeQuery()
	if err != nil {
		return res, err
	}

	if offset > 0 {
		st = st.Offset(offset)
	}

	if limit >= 0 {
		st = st.Limit(limit)
	}

	st = st.Map(func(d document.Document) (document.Document, error) {
		return documentMask{
			cfg:          qo.cfg,
			r:            d,
			resultFields: stmt.Selectors,
		}, nil
	})

	return Result{Stream: st}, nil
}

type documentMask struct {
	cfg          *database.TableConfig
	r            document.Document
	resultFields []ResultField
}

var _ document.Document = documentMask{}

func (r documentMask) GetByField(name string) (document.Value, error) {
	for _, rf := range r.resultFields {
		if rf.Name() == name || rf.Name() == "*" {
			return r.r.GetByField(name)
		}
	}

	return document.Value{}, document.ErrFieldNotFound
}

func (r documentMask) Iterate(fn func(f string, v document.Value) error) error {
	stack := EvalStack{
		Document: r.r,
		Cfg:      r.cfg,
	}

	for _, rf := range r.resultFields {
		err := rf.Iterate(stack, fn)
		if err != nil {
			return err
		}
	}

	return nil
}

// A ResultField is a field that will be part of the result document that will be returned at the end of a Select statement.
type ResultField interface {
	Iterate(stack EvalStack, fn func(field string, value document.Value) error) error
	Name() string
}

// ResultFieldExpr turns any expression into a ResultField.
type ResultFieldExpr struct {
	Expr

	ExprName string
}

// Name returns the raw expression.
func (r ResultFieldExpr) Name() string {
	return r.ExprName
}

// Iterate evaluates Expr and calls fn once with the result.
func (r ResultFieldExpr) Iterate(stack EvalStack, fn func(field string, value document.Value) error) error {
	v, err := r.Expr.Eval(stack)
	if err != nil && err != document.ErrFieldNotFound {
		return err
	}

	return fn(r.ExprName, v)
}

// A Wildcard is a ResultField that iterates over all the fields of a document.
type Wildcard struct{}

// Name returns the "*" character.
func (w Wildcard) Name() string {
	return "*"
}

// Iterate call the document iterate method.
func (w Wildcard) Iterate(stack EvalStack, fn func(fd string, v document.Value) error) error {
	if stack.Document == nil {
		return errors.New("no table specified")
	}

	return stack.Document.Iterate(fn)
}
