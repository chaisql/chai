package planner

import (
	"github.com/genjidb/genji/key"
	"hash"
	"hash/maphash"

	"github.com/genjidb/genji/document"
)

type valueDocumentHashMap struct {
	hash hash.Hash64
	m    map[uint64]document.Document
}

func newDocumentHashMap(hash hash.Hash64) *valueDocumentHashMap {
	if hash == nil {
		hash = &maphash.Hash{}
	}

	m := map[uint64]document.Document{}
	return &valueDocumentHashMap{
		hash: hash,
		m:    m,
	}
}

func (s valueDocumentHashMap) generateKey(d document.Value) (uint64, error) {
	defer s.hash.Reset()

	var buf []byte
	buf, err := key.AppendValue(buf, d)
	if err != nil {
		return 0, err
	}

	_, err = s.hash.Write(buf)
	if err != nil {
		return 0, err
	}

	return s.hash.Sum64(), nil
}

func (s valueDocumentHashMap) Get(k document.Value) (document.Document, error) {
	docHash, err := s.generateKey(k)
	if err != nil {
		return nil, err
	}

	v, ok := s.m[docHash]
	if !ok {
		return nil, document.ErrValueNotFound
	}
	return v, nil
}

func (s valueDocumentHashMap) Add(k document.Value, v document.Document) error {
	docHash, err := s.generateKey(k)
	if err != nil {
		return err
	}

	fb := document.NewFieldBuffer()
	err = fb.Copy(v)
	if err != nil {
		return err
	}

	s.m[docHash] = fb
	return nil
}
