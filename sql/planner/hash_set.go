package planner

import (
	"hash"
	"hash/maphash"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/key"
)

type documentHashSet struct {
	hash hash.Hash64
	set  map[uint64]struct{}
}

func newDocumentHashSet(hash hash.Hash64) *documentHashSet {
	if hash == nil {
		hash = &maphash.Hash{}
	}

	return &documentHashSet{
		hash: hash,
		set:  map[uint64]struct{}{},
	}
}

func (s documentHashSet) generateKey(d document.Document) (uint64, error) {
	defer s.hash.Reset()

	fields, err := document.Fields(d)
	if err != nil {
		return 0, err
	}

	for _, field := range fields {
		value, err := d.GetByField(field)
		if err != nil {
			return 0, err
		}

		buf, err := key.AppendValue(nil, value)
		if err != nil {
			return 0, err
		}

		_, err = s.hash.Write(buf)
		if err != nil {
			return 0, err
		}
	}

	return s.hash.Sum64(), nil
}

func (s documentHashSet) Filter(d document.Document) (bool, error) {
	k, err := s.generateKey(d)
	if err != nil {
		return false, err
	}

	_, ok := s.set[k]
	if ok {
		return false, nil
	}

	s.set[k] = struct{}{}
	return true, nil
}
