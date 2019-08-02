package memory

import (
	"bytes"
	"errors"

	"github.com/asdine/genji/engine"
	"github.com/google/btree"
)

type item struct {
	k, v []byte
}

func (i *item) Less(than btree.Item) bool {
	return bytes.Compare(i.k, than.(*item).k) < 0
}

type storeTx struct {
	tr *btree.BTree
	tx *transaction
}

func (s *storeTx) Put(k, v []byte) error {
	if !s.tx.writable {
		return engine.ErrTransactionReadOnly
	}

	if len(k) == 0 {
		return errors.New("empty keys are forbidden")
	}

	it := item{k, v}
	s.tr.ReplaceOrInsert(&it)

	s.tx.undos = append(s.tx.undos, func() {
		s.tr.Delete(&it)
	})

	return nil
}

func (s *storeTx) Get(k []byte) ([]byte, error) {
	it := s.tr.Get(&item{k: k})

	if it == nil {
		return nil, engine.ErrKeyNotFound
	}

	return it.(*item).v, nil
}

func (s *storeTx) Delete(k []byte) error {
	if !s.tx.writable {
		return engine.ErrTransactionReadOnly
	}

	it := s.tr.Get(&item{k: k})
	if it == nil {
		return engine.ErrKeyNotFound
	}

	s.tr.Delete(it)

	s.tx.undos = append(s.tx.undos, func() {
		s.tr.ReplaceOrInsert(it)
	})

	return nil
}

func (s *storeTx) Truncate() error {
	if !s.tx.writable {
		return engine.ErrTransactionReadOnly
	}

	old := s.tr
	s.tr = btree.New(3)

	s.tx.undos = append(s.tx.undos, func() {
		s.tr = old
	})

	return nil
}

func (s *storeTx) AscendGreaterOrEqual(start []byte, fn func(k, v []byte) error) (err error) {
	s.tr.AscendGreaterOrEqual(&item{k: start}, btree.ItemIterator(func(i btree.Item) bool {
		it := i.(*item)
		err = fn(it.k, it.v)
		return err == nil
	}))

	return
}

func (s *storeTx) DescendLessOrEqual(start []byte, fn func(k, v []byte) error) (err error) {
	if start == nil {
		s.tr.Descend(btree.ItemIterator(func(i btree.Item) bool {
			it := i.(*item)
			err = fn(it.k, it.v)
			return err == nil
		}))
		return
	}

	s.tr.DescendLessOrEqual(&item{k: start}, btree.ItemIterator(func(i btree.Item) bool {
		it := i.(*item)
		err = fn(it.k, it.v)
		return err == nil
	}))

	return
}
