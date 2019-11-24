package query

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/record"
	"github.com/asdine/genji/value"
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

		if v.Value.Type < value.Int {
			return res, fmt.Errorf("offset expression must evaluate to an integer, got %q", v.Value.Type)
		}

		voff, err := v.Value.ConvertTo(value.Int)
		if err != nil {
			return res, err
		}
		offset, err = value.DecodeInt(voff.Data)
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

		if v.Value.Type < value.Int {
			return res, fmt.Errorf("limit expression must evaluate to an integer, got %q", v.Value.Type)
		}

		vlim, err := v.Value.ConvertTo(value.Int)
		if err != nil {
			return res, err
		}
		limit, err = value.DecodeInt(vlim.Data)
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

	st = st.Map(func(r record.Record) (record.Record, error) {
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
	r            record.Record
	resultFields []ResultField
}

var _ record.Record = RecordMask{}

func (r RecordMask) GetField(name string) (record.Field, error) {
	for _, rf := range r.resultFields {
		if rf.Name() == name || rf.Name() == "*" {
			return r.r.GetField(name)
		}
	}

	return record.Field{}, fmt.Errorf("field %q not found", name)
}

func (r RecordMask) Iterate(fn func(f record.Field) error) error {
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
	Iterate(stack EvalStack, fn func(fd record.Field) error) error
	Name() string
}

type FieldSelector string

func (f FieldSelector) Name() string {
	return string(f)
}

func (f FieldSelector) SelectField(r record.Record) (record.Field, error) {
	if r == nil {
		return record.Field{}, fmt.Errorf("field %q not found", f)
	}

	return r.GetField(string(f))
}

func (f FieldSelector) Iterate(stack EvalStack, fn func(fd record.Field) error) error {
	fd, err := f.SelectField(stack.Record)
	if err != nil {
		return nil
	}

	return fn(fd)
}

// Eval extracts the record from the context and selects the right field.
// It implements the Expr interface.
func (f FieldSelector) Eval(stack EvalStack) (EvalValue, error) {
	if stack.Record == nil {
		return EvalValue{}, fmt.Errorf("field %q not found", f)
	}

	fd, err := f.SelectField(stack.Record)
	if err != nil {
		return nilLitteral, nil
	}

	return newSingleEvalValue(fd.Value), nil
}

type Wildcard struct{}

func (w Wildcard) Name() string {
	return "*"
}

func (w Wildcard) Iterate(stack EvalStack, fn func(fd record.Field) error) error {
	return stack.Record.Iterate(fn)
}

type KeyFunc struct{}

func (k KeyFunc) Name() string {
	return "key()"
}

func (k KeyFunc) Iterate(stack EvalStack, fn func(fd record.Field) error) error {
	if stack.Cfg.PrimaryKeyName != "" {
		fd, err := stack.Record.GetField(stack.Cfg.PrimaryKeyName)
		if err != nil {
			return err
		}
		return fn(fd)
	}

	return fn(record.Field{
		Name: "key()",
		Value: value.Value{
			Data: stack.Record.(record.Keyer).Key(),
			Type: value.Int64,
		},
	})
}
