package query

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/document/encoding"
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
func (stmt SelectStmt) Run(tx *database.Transaction, args []driver.NamedValue) (Result, error) {
	return stmt.exec(tx, args)
}

// Exec the Select query within tx.
func (stmt SelectStmt) exec(tx *database.Transaction, args []driver.NamedValue) (Result, error) {
	var res Result

	if stmt.TableName == "" {
		return res, errors.New("missing table selector")
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

		voff, err := v.ConvertTo(document.IntValue)
		if err != nil {
			return res, err
		}
		offset, err = voff.ConvertToInt()
		if err != nil {
			return res, err
		}
	}

	if stmt.LimitExpr != nil {
		v, err := stmt.LimitExpr.Eval(stack)
		if err != nil {
			return res, err
		}

		if !v.Type.IsNumber() {
			return res, fmt.Errorf("limit expression must evaluate to a number, got %q", v.Type)
		}

		vlim, err := v.ConvertTo(document.IntValue)
		if err != nil {
			return res, err
		}
		limit, err = vlim.ConvertToInt()
		if err != nil {
			return res, err
		}
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

// A FieldSelector is a ResultField that extracts a field from a document at a given path.
type FieldSelector []string

// Name joins the chunks of the path of the field selector using the dot separator.
func (f FieldSelector) Name() string {
	return strings.Join(f, ".")
}

func (f FieldSelector) selectField(d document.Document) (string, document.Value, error) {
	if d == nil {
		return f.Name(), nilLitteral, document.ErrFieldNotFound
	}

	var v document.Value
	var a document.Array
	var err error

	for i, chunk := range f {
		if d != nil {
			v, err = d.GetByField(chunk)
		} else {
			idx, err := strconv.Atoi(chunk)
			if err != nil {
				return f.Name(), nilLitteral, document.ErrFieldNotFound
			}
			v, err = a.GetByIndex(idx)
		}
		if err != nil {
			return f.Name(), nilLitteral, err
		}

		if i+1 == len(f) {
			break
		}

		d = nil
		a = nil

		switch v.Type {
		case document.DocumentValue:
			d, err = v.ConvertToDocument()
		case document.ArrayValue:
			a, err = v.ConvertToArray()
		default:
			return f.Name(), nilLitteral, document.ErrFieldNotFound
		}
		if err != nil {
			return f.Name(), nilLitteral, err
		}
	}

	return f.Name(), v, nil
}

// Iterate select the field from the document and calls fn once with this field.
func (f FieldSelector) Iterate(stack EvalStack, fn func(fd string, v document.Value) error) error {
	fd, v, err := f.selectField(stack.Document)
	if err != nil && err != document.ErrFieldNotFound {
		return err
	}

	return fn(fd, v)
}

// Eval extracts the document from the context and selects the right field.
// It implements the Expr interface.
func (f FieldSelector) Eval(stack EvalStack) (document.Value, error) {
	if stack.Document == nil {
		return nilLitteral, document.ErrFieldNotFound
	}

	_, v, err := f.selectField(stack.Document)
	if err != nil {
		return nilLitteral, document.ErrFieldNotFound
	}

	return v, nil
}

// A Wildcard is a ResultField that iterates over all the fields of a document.
type Wildcard struct{}

// Name returns the "*" character.
func (w Wildcard) Name() string {
	return "*"
}

// Iterate call the document iterate method.
func (w Wildcard) Iterate(stack EvalStack, fn func(fd string, v document.Value) error) error {
	return stack.Document.Iterate(fn)
}

// KeyFunc is a function that returns the primary key corresponding to the current document.
type KeyFunc struct{}

// Name returns "key()".
func (k KeyFunc) Name() string {
	return "key()"
}

// Iterate identifies the primary key for the document and calls fn with it.
func (k KeyFunc) Iterate(stack EvalStack, fn func(fd string, v document.Value) error) error {
	if len(stack.Cfg.PrimaryKey.Path) != 0 {
		v, err := stack.Cfg.PrimaryKey.Path.GetValue(stack.Document)
		if err != nil {
			return err
		}
		return fn(stack.Cfg.PrimaryKey.Path.String(), v)
	}

	v, err := encoding.DecodeValue(document.Int64Value, stack.Document.(document.Keyer).Key())
	if err != nil {
		return err
	}

	return fn(k.Name(), v)
}
