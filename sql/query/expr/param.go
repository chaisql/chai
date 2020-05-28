package expr

import (
	"fmt"

	"github.com/genjidb/genji/document"
)

// A Param represents a parameter passed by the user to the statement.
type Param struct {
	// Name of the param
	Name string

	// Value is the parameter value.
	Value interface{}
}

// NamedParam is an expression which represents the name of a parameter.
type NamedParam string

// Eval looks up for the parameters in the stack for the one that has the same name as p
// and returns the value.
func (p NamedParam) Eval(stack EvalStack) (document.Value, error) {
	v, err := p.extract(stack.Params)
	if err != nil {
		return nullLitteral, err
	}

	return document.NewValue(v)
}

func (p NamedParam) extract(params []Param) (interface{}, error) {
	for _, nv := range params {
		if nv.Name == string(p) {
			return nv.Value, nil
		}
	}

	return nil, fmt.Errorf("param %s not found", p)
}

// PositionalParam is an expression which represents the position of a parameter.
type PositionalParam int

// Eval looks up for the parameters in the stack for the one that is has the same position as p
// and returns the value.
func (p PositionalParam) Eval(stack EvalStack) (document.Value, error) {
	v, err := p.extract(stack.Params)
	if err != nil {
		return nullLitteral, err
	}

	return document.NewValue(v)
}

func (p PositionalParam) extract(params []Param) (interface{}, error) {
	idx := int(p - 1)
	if idx >= len(params) {
		return nil, fmt.Errorf("can't find param number %d", p)
	}

	return params[idx].Value, nil
}
