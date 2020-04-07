package query

import (
	"errors"

	"github.com/asdine/genji/document"
)

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
