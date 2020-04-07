// +build !wasm

package query

import "github.com/asdine/genji/document"

func extractDocumentFromParamExtractor(pe paramExtractor, params []Param) (document.Document, error) {
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
