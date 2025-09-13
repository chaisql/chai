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
	Value any
}

// Environment contains information about the context in which
// the expression is evaluated.
type Environment struct {
	db     *database.Database
	tx     *database.Transaction
	params []Param
	row    row.Row
}

func New(db *database.Database, tx *database.Transaction, params []Param, row row.Row) *Environment {
	env := Environment{
		db:     db,
		tx:     tx,
		params: params,
		row:    row,
	}

	return &env
}

func (e *Environment) Clone(r row.Row) *Environment {
	return &Environment{
		db:     e.db,
		tx:     e.tx,
		params: e.params,
		row:    r,
	}
}

func (e *Environment) GetRow() (row.Row, bool) {
	return e.row, e.row != nil
}

func (e *Environment) GetParamByIndex(pos int) (types.Value, error) {
	idx := int(pos - 1)
	if idx >= len(e.params) {
		return nil, fmt.Errorf("cannot find param number %d", pos)
	}

	return row.NewValue(e.params[idx].Value)
}

func (e *Environment) GetTx() *database.Transaction {
	return e.tx
}

func (e *Environment) GetDB() *database.Database {
	return e.db
}
