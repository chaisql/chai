package expr

import (
	"fmt"

	"github.com/genjidb/genji/document"
)

const (
	currentValueKey = "$v"
	paramsKey       = "$params"
)

// Environment contains information about the context in which
// the expression is evaluated.
type Environment struct {
	V      document.Value
	Params []Param
}

func (e *Environment) GetParamByName(name string) (document.Value, error) {
	for _, nv := range e.Params {
		if nv.Name == name {
			return document.NewValue(nv.Value)
		}
	}

	return document.Value{}, fmt.Errorf("param %s not found", name)
}

func (e *Environment) GetParamByIndex(pos int) (document.Value, error) {
	idx := int(pos - 1)
	if idx >= len(e.Params) {
		return document.Value{}, fmt.Errorf("cannot find param number %d", pos)
	}

	return document.NewValue(e.Params[idx].Value)
}
