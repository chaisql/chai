package expr

import (
	"fmt"

	"github.com/genjidb/genji/document"
)

const (
	currentValueKey = "_v"
)

// Environment contains information about the context in which
// the expression is evaluated.
type Environment struct {
	Params []Param
	Buf    *document.FieldBuffer

	Outer *Environment
}

func NewEnvironment(v document.Value, params ...Param) *Environment {
	env := Environment{
		Params: params,
	}

	if v.Type != 0 {
		env.Buf = document.NewFieldBuffer()
		env.Set(currentValueKey, v)
	}

	return &env
}

func (e *Environment) Get(path document.Path) (v document.Value, ok bool) {
	if e.Buf != nil {
		v, err := path.GetValueFromDocument(e.Buf)
		if err == nil {
			return v, true
		}

		v, err = e.Buf.GetByField(currentValueKey)
		if err == nil {
			v, err = path.GetValue(v)
			if err == nil {
				return v, true
			}
		}
	}

	if e.Outer != nil {
		return e.Outer.Get(path)
	}

	return
}

func (e *Environment) Set(name string, v document.Value) {
	if e.Buf == nil {
		e.Buf = document.NewFieldBuffer()
	}

	e.Buf.Set(document.Path{document.PathFragment{FieldName: name}}, v)
}

func (e *Environment) GetCurrentValue() (document.Value, bool) {
	return e.Get(document.Path{document.PathFragment{FieldName: currentValueKey}})
}

func (e *Environment) SetCurrentValue(v document.Value) {
	e.Set(currentValueKey, v)
}

func (e *Environment) GetParamByName(name string) (v document.Value, err error) {
	if len(e.Params) == 0 {
		if e.Outer != nil {
			return e.Outer.GetParamByName(name)
		}
	}

	for _, nv := range e.Params {
		if nv.Name == name {
			return document.NewValue(nv.Value)
		}
	}

	return document.Value{}, fmt.Errorf("param %s not found", name)
}

func (e *Environment) GetParamByIndex(pos int) (document.Value, error) {
	if len(e.Params) == 0 {
		if e.Outer != nil {
			return e.Outer.GetParamByIndex(pos)
		}
	}

	idx := int(pos - 1)
	if idx >= len(e.Params) {
		return document.Value{}, fmt.Errorf("cannot find param number %d", pos)
	}

	return document.NewValue(e.Params[idx].Value)
}
