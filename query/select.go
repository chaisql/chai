package query

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/document"
)

// SelectStmt is a DSL that allows creating a full Select query.
type SelectStmt struct {
	TableName  string
	WhereExpr  Expr
	OffsetExpr Expr
	LimitExpr  Expr
	Selectors  []ResultField
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

	t, err := tx.GetTable(stmt.TableName)
	if err != nil {
		return res, err
	}

	indexes, err := t.Indexes()
	if err != nil {
		return res, err
	}

	cfg, err := t.CfgStore.Get(t.TableName())
	if err != nil {
		return res, err
	}

	qo := queryOptimizer{
		tx:        tx,
		t:         t,
		whereExpr: stmt.WhereExpr,
		args:      args,
		cfg:       cfg,
		indexes:   indexes,
	}

	st, err := qo.optimizeQuery()
	if err != nil {
		return res, err
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

		if v.IsList {
			return res, fmt.Errorf("expected value got list")
		}

		if v.Value.Type < document.IntValue {
			return res, fmt.Errorf("offset expression must evaluate to an integer, got %q", v.Value.Type)
		}

		voff, err := v.Value.ConvertTo(document.IntValue)
		if err != nil {
			return res, err
		}
		offset, err = document.DecodeInt(voff.Data)
		if err != nil {
			return res, err
		}
	}

	if stmt.LimitExpr != nil {
		v, err := stmt.LimitExpr.Eval(stack)
		if err != nil {
			return res, err
		}

		if v.IsList {
			return res, fmt.Errorf("expected value got list")
		}

		if v.Value.Type < document.IntValue {
			return res, fmt.Errorf("limit expression must evaluate to an integer, got %q", v.Value.Type)
		}

		vlim, err := v.Value.ConvertTo(document.IntValue)
		if err != nil {
			return res, err
		}
		limit, err = document.DecodeInt(vlim.Data)
		if err != nil {
			return res, err
		}
	}

	st = st.Filter(whereClause(stmt.WhereExpr, stack))

	if offset > 0 {
		st = st.Offset(offset)
	}

	if limit >= 0 {
		st = st.Limit(limit)
	}

	st = st.Map(func(r document.Document) (document.Document, error) {
		return RecordMask{
			cfg:          cfg,
			r:            r,
			resultFields: stmt.Selectors,
		}, nil
	})

	return Result{Stream: st}, nil
}

type RecordMask struct {
	cfg          *database.TableConfig
	r            document.Document
	resultFields []ResultField
}

var _ document.Document = RecordMask{}

func (r RecordMask) GetByField(name string) (document.Value, error) {
	for _, rf := range r.resultFields {
		if rf.Name() == name || rf.Name() == "*" {
			return r.r.GetByField(name)
		}
	}

	return document.Value{}, fmt.Errorf("field %q not found", name)
}

func (r RecordMask) Iterate(fn func(f string, v document.Value) error) error {
	stack := EvalStack{
		Record: r.r,
		Cfg:    r.cfg,
	}

	for _, rf := range r.resultFields {
		err := rf.Iterate(stack, fn)
		if err != nil {
			return err
		}
	}

	return nil
}

type ResultField interface {
	Iterate(stack EvalStack, fn func(field string, value document.Value) error) error
	Name() string
}

type FieldSelector []string

func (f FieldSelector) Name() string {
	return strings.Join(f, ".")
}

func (f FieldSelector) SelectField(d document.Document) (string, document.Value, error) {
	if d == nil {
		return "", document.Value{}, fmt.Errorf("field %q not found", f)
	}

	var v document.Value
	var err error

	for i, chunk := range f {
		v, err = d.GetByField(chunk)
		if err != nil {
			return "", document.Value{}, err
		}

		if i+1 == len(f) {
			break
		}

		if v.Type != document.DocumentValue {
			return f.Name(), document.Value{}, fmt.Errorf("field %q not found", f.Name())
		}

		d, err = v.DecodeToDocument()
		if err != nil {
			return "", document.Value{}, err
		}
	}

	return f.Name(), v, nil
}

func (f FieldSelector) Iterate(stack EvalStack, fn func(fd string, v document.Value) error) error {
	fd, v, err := f.SelectField(stack.Record)
	if err != nil {
		return nil
	}

	return fn(fd, v)
}

// Eval extracts the record from the context and selects the right field.
// It implements the Expr interface.
func (f FieldSelector) Eval(stack EvalStack) (EvalValue, error) {
	if stack.Record == nil {
		return EvalValue{}, fmt.Errorf("field %q not found", f)
	}

	_, v, err := f.SelectField(stack.Record)
	if err != nil {
		return nilLitteral, nil
	}

	return newSingleEvalValue(v), nil
}

type Wildcard struct{}

func (w Wildcard) Name() string {
	return "*"
}

func (w Wildcard) Iterate(stack EvalStack, fn func(fd string, v document.Value) error) error {
	return stack.Record.Iterate(fn)
}

type KeyFunc struct{}

func (k KeyFunc) Name() string {
	return "key()"
}

func (k KeyFunc) Iterate(stack EvalStack, fn func(fd string, v document.Value) error) error {
	if stack.Cfg.PrimaryKeyName != "" {
		v, err := stack.Record.GetByField(stack.Cfg.PrimaryKeyName)
		if err != nil {
			return err
		}
		return fn(stack.Cfg.PrimaryKeyName, v)
	}

	return fn("key()", document.Value{
		Data: stack.Record.(document.Keyer).Key(),
		Type: document.Int64Value,
	})
}
