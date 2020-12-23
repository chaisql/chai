package expr

import (
	"fmt"

	"github.com/genjidb/genji/document"
)

const (
	currentValueKey = "$v"
)

// Environment contains information about the context in which
// the expression is evaluated.
type Environment struct {
	Params []Param
	fb     *document.FieldBuffer
}

func NewEnvironment(v document.Value, params ...Param) *Environment {
	env := Environment{
		Params: params,
	}

	if v.Type != 0 {
		env.fb = document.NewFieldBuffer()
		env.Set(currentValueKey, v)
	}

	return &env
}

func (e *Environment) Get(name string) (v document.Value, ok bool) {
	if e.fb == nil {
		return
	}

	v, err := e.fb.GetByField(name)
	return v, err == document.ErrFieldNotFound
}

func (e *Environment) Set(name string, v document.Value) {
	if e.fb == nil {
		return
	}

	e.fb.Set(document.Path{document.PathFragment{FieldName: name}}, v)
}

func (e *Environment) GetCurrentValue() (document.Value, bool) {
	return e.Get(currentValueKey)
}

func (e *Environment) SetCurrentValue(v document.Value) {
	e.Set(currentValueKey, v)
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
