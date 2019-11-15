package memoryengine

import (
	"bytes"
	"errors"

	"github.com/asdine/genji/engine"
	"github.com/google/btree"
)

type item struct {
	k, v    []byte
	deleted bool
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

	it := &item{k: k}
	if i := s.tr.Get(it); i != nil {
		cur := i.(*item)

		oldv, oldDeleted := cur.v, cur.deleted
		cur.v = v
		cur.deleted = false

		s.tx.onRollback = append(s.tx.onRollback, func() {
			cur.v = oldv
			cur.deleted = oldDeleted
		})

		return nil
	}

	it.v = v
	s.tr.ReplaceOrInsert(it)

	s.tx.onRollback = append(s.tx.onRollback, func() {
		s.tr.Delete(it)
	})

	return nil
}

func (s *storeTx) Get(k []byte) ([]byte, error) {
	it := s.tr.Get(&item{k: k})

	if it == nil {
		return nil, engine.ErrKeyNotFound
	}

	i := it.(*item)
	if i.deleted {
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

	i := it.(*item)
	if i.deleted {
		return engine.ErrKeyNotFound
	}

	i.deleted = true

	s.tx.onRollback = append(s.tx.onRollback, func() {
		i.deleted = false
	})

	s.tx.onCommit = append(s.tx.onCommit, func() {
		s.tr.Delete(i)
	})
	return nil
}

func (s *storeTx) Truncate() error {
	if !s.tx.writable {
		return engine.ErrTransactionReadOnly
	}

	old := s.tr
	s.tr = btree.New(3)

	s.tx.onRollback = append(s.tx.onRollback, func() {
		s.tr = old
	})

	return nil
}

func (s *storeTx) AscendGreaterOrEqual(start []byte, fn func(k, v []byte) error) (err error) {
	iterator := btree.ItemIterator(func(i btree.Item) bool {
		it := i.(*item)
		if it.deleted {
			return true
		}
		err = fn(it.k, it.v)
		return err == nil
	})

	if len(start) == 0 {
		s.tr.Ascend(iterator)
	} else {
		s.tr.AscendGreaterOrEqual(&item{k: start}, iterator)
	}

	return
}

func (s *storeTx) DescendLessOrEqual(pivot []byte, fn func(k, v []byte) error) (err error) {
	if pivot == nil {
		s.tr.Descend(btree.ItemIterator(func(i btree.Item) bool {
			it := i.(*item)
			if it.deleted {
				return true
			}
			err = fn(it.k, it.v)
			return err == nil
		}))
		return
	}

	s.tr.DescendLessOrEqual(&item{k: pivot}, btree.ItemIterator(func(i btree.Item) bool {
		it := i.(*item)
		if it.deleted {
			return true
		}
		err = fn(it.k, it.v)
		return err == nil
	}))

	return
}
