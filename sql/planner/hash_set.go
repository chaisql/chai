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

	err := d.Iterate(func(field string, value document.Value) error {
		var buf []byte
		buf, err := key.AppendValue(buf, value)
		if err != nil {
			return err
		}

		_, err = s.hash.Write(buf)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return 0, err
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
