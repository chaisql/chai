package environment

import (
	"fmt"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/object"
	"github.com/chaisql/chai/internal/types"
)

// A Param represents a parameter passed by the user to the statement.
type Param struct {
	// Name of the param
	Name string

	// Value is the parameter value.
	Value any
}

// Environment contains information about the context in which
// the expression is evaluated.
type Environment struct {
	Params []Param
	Bloc   Bloc
	DB     *database.Database
	Tx     *database.Transaction

	Outer *Environment
}

func New(b Bloc, params ...Param) *Environment {
	env := Environment{
		Params: params,
		Bloc:   b,
	}

	return &env
}

func (e *Environment) GetOuter() *Environment {
	return e.Outer
}

func (e *Environment) SetOuter(env *Environment) {
	e.Outer = env
}

func (e *Environment) GetBloc() (Bloc, bool) {
	if e.Bloc != nil {
		return e.Bloc, true
	}

	if e.Outer != nil {
		return e.Outer.GetBloc()
	}

	return nil, false
}

func (e *Environment) SetBloc(b Bloc) {
	e.Bloc = b
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
			return object.NewValue(nv.Value)
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

	return object.NewValue(e.Params[idx].Value)
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

type Bloc interface {
	Next() database.Row
	Len() int
	Close() error
}
