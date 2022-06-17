package environment

import (
	"fmt"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
)

var (
	TableKey = document.Path{document.PathFragment{FieldName: "$table"}}
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
	Params  []Param
	Vars    *document.FieldBuffer
	Key     *tree.Key
	Doc     types.Document
	DB      *database.Database
	Catalog *database.Catalog
	Tx      *database.Transaction

	Outer *Environment
}

func New(d types.Document, params ...Param) *Environment {
	env := Environment{
		Params: params,
		Doc:    d,
	}

	return &env
}

func (e *Environment) GetOuter() *Environment {
	return e.Outer
}

func (e *Environment) SetOuter(env *Environment) {
	e.Outer = env
}

func (e *Environment) Get(path document.Path) (v types.Value, ok bool) {
	if e.Vars != nil {
		v, err := path.GetValueFromDocument(e.Vars)
		if err == nil {
			return v, true
		}
	}

	if e.Outer != nil {
		return e.Outer.Get(path)
	}

	return types.NewNullValue(), false
}

func (e *Environment) Set(path document.Path, v types.Value) {
	if e.Vars == nil {
		e.Vars = document.NewFieldBuffer()
	}

	e.Vars.Set(path, v)
}

func (e *Environment) GetDocument() (types.Document, bool) {
	if e.Doc != nil {
		return e.Doc, true
	}

	if e.Outer != nil {
		return e.Outer.GetDocument()
	}

	return nil, false
}

func (e *Environment) SetDocument(d types.Document) {
	e.Doc = d
}

func (e *Environment) GetKey() (*tree.Key, bool) {
	if e.Key != nil {
		return e.Key, true
	}

	if e.Outer != nil {
		return e.Outer.GetKey()
	}

	return nil, false
}

func (e *Environment) SetKey(k *tree.Key) {
	e.Key = k
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
			return document.NewValue(nv.Value)
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

	return document.NewValue(e.Params[idx].Value)
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

func (e *Environment) GetCatalog() *database.Catalog {
	if e.Catalog != nil {
		return e.Catalog
	}
	if outer := e.GetOuter(); outer != nil {
		return outer.GetCatalog()
	}

	return nil
}
