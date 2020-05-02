// +build !wasm

package query

import (
	"github.com/asdine/genji/document"
	"github.com/asdine/genji/sql/query/expr"
)

func extractDocumentFromParamExtractor(pe paramExtractor, params []expr.Param) (document.Document, error) {
	v, err := pe.extract(params)
	if err != nil {
		return nil, err
	}

	var ok bool
	d, ok := v.(document.Document)
	if !ok {
		d, err = document.NewFromStruct(v)
		if err != nil {
			return nil, err
		}
	}

	return d, nil
}
