package query

import (
	"errors"

	"github.com/asdine/genji/document"
)

// Eval looks up for the parameters in the stack for the one that has the same name as p
// and returns the value.
func (p NamedParam) Eval(stack EvalStack) (document.Value, error) {
	v, err := p.extract(stack.Params)
	if err != nil {
		return nilLitteral, err
	}

	return document.NewValue(v)
}

// Eval looks up for the parameters in the stack for the one that is has the same position as p
// and returns the value.
func (p PositionalParam) Eval(stack EvalStack) (document.Value, error) {
	v, err := p.extract(stack.Params)
	if err != nil {
		return nilLitteral, err
	}

	return document.NewValue(v)
}

func extractDocumentFromParamExtractor(pe paramExtractor, params []Param) (document.Document, error) {
	v, err := pe.extract(params)
	if err != nil {
		return nil, err
	}

	var ok bool
	d, ok := v.(document.Document)
	if !ok {
		return nil, errors.New("parameter must be a document")
	}

	return d, nil
}
