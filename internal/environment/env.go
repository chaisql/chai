package environment

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/row"
	"github.com/chaisql/chai/internal/types"
)

// A Param represents a parameter passed by the user to the statement.
type Param struct {
	// Name of the param
	Name string

	// Value is the parameter value.
	Value interface{}
}

// Environment contains information about the context in which
// the expression is evaluated.
type Environment struct {
	Params []Param
	Vars   *row.ColumnBuffer
	Row    row.Row
	DB     *database.Database
	Tx     *database.Transaction

	Outer *Environment
}

func New(r database.Row, params ...Param) *Environment {
	env := Environment{
		Params: params,
		Row:    r,
	}

	return &env
}

func (e *Environment) GetOuter() *Environment {
	return e.Outer
}

func (e *Environment) SetOuter(env *Environment) {
	e.Outer = env
}

func (e *Environment) Get(column string) (v types.Value, ok bool) {
	if e.Vars != nil {
		v, err := e.Vars.Get(column)
		if err == nil {
			return v, true
		}
	}

	if e.Outer != nil {
		return e.Outer.Get(column)
	}

	return types.NewNullValue(), false
}

func (e *Environment) Set(column string, v types.Value) {
	if e.Vars == nil {
		e.Vars = row.NewColumnBuffer()
	}

	e.Vars.Set(column, v)
}

func (e *Environment) GetRow() (row.Row, bool) {
	if e.Row != nil {
		return e.Row, true
	}

	if e.Outer != nil {
		return e.Outer.GetRow()
	}

	return nil, false
}

func (e *Environment) GetDatabaseRow() (database.Row, bool) {
	if e.Row != nil {
		r, ok := e.Row.(database.Row)
		return r, ok
	}

	if e.Outer != nil {
		return e.Outer.GetDatabaseRow()
	}

	return nil, false
}

func (e *Environment) SetRow(r row.Row) {
	e.Row = r
}

func (e *Environment) SetParams(params []Param) {
	e.Params = params
}

func (e *Environment) GetParamByName(name string) (v types.Value, err error) {
	if len(e.Params) == 0 {
		if e.Outer != nil {
			return e.Outer.GetParamByName(name)
		}
	}

	for _, nv := range e.Params {
		if nv.Name == name {
			return row.NewValue(nv.Value)
		}
	}

	return nil, fmt.Errorf("param %s not found", name)
}

func (e *Environment) GetParamByIndex(pos int) (types.Value, error) {
	if len(e.Params) == 0 {
		if e.Outer != nil {
			return e.Outer.GetParamByIndex(pos)
		}
	}

	idx := int(pos - 1)
	if idx >= len(e.Params) {
		return nil, fmt.Errorf("cannot find param number %d", pos)
	}

	return row.NewValue(e.Params[idx].Value)
}

func (e *Environment) GetTx() *database.Transaction {
	if e.Tx != nil {
		return e.Tx
	}

	if outer := e.GetOuter(); outer != nil {
		return outer.GetTx()
	}

	return nil
}

func (e *Environment) GetDB() *database.Database {
	if e.DB != nil {
		return e.DB
	}

	if outer := e.GetOuter(); outer != nil {
		return outer.GetDB()
	}

	return nil
}
