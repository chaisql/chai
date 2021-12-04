package memoryengine

import (
	"bytes"
	"context"

	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/internal/errors"
	"github.com/google/btree"
)

// item implements an engine.Item.
// it is also used as a btree.Item.
type item struct {
	k, v []byte
	// set to true if the item has been deleted
	// during the current transaction
	// but before rollback or commit.
	deleted bool
}

func (i *item) Key() []byte {
	return i.k
}

func (i *item) ValueCopy(buf []byte) ([]byte, error) {
	if len(buf) < len(i.v) {
		buf = make([]byte, len(i.v))
	}
	n := copy(buf, i.v)
	return buf[:n], nil
}

func (i *item) Less(than btree.Item) bool {
	return bytes.Compare(i.k, than.(*item).k) < 0
}

// storeTx implements an engine.Store.
type storeTx struct {
	tr   *btree.BTree
	tx   *transaction
	name string
}

func (s *storeTx) Put(k, v []byte) error {
	select {
	case <-s.tx.ctx.Done():
		return s.tx.ctx.Err()
	default:
	}

	if !s.tx.writable {
		return errors.Wrap(engine.ErrTransactionReadOnly)
	}

	if len(k) == 0 {
		return errors.New("empty keys are forbidden")
	}

	if len(v) == 0 {
		return errors.New("empty values are forbidden")
	}

	it := &item{k: k}
	// if there is an existing value, fetch it
	// and overwrite it directly using the pointer.
	if i := s.tr.Get(it); i != nil {
		cur := i.(*item)

		oldv, oldDeleted := cur.v, cur.deleted
		cur.v = v
		cur.deleted = false

		// on rollback replace the new value by the old value
		s.tx.onRollback = append(s.tx.onRollback, func() {
			cur.v = oldv
			cur.deleted = oldDeleted
		})

		return nil
	}

	it.v = v
	s.tr.ReplaceOrInsert(it)

	// on rollback delete the new item
	s.tx.onRollback = append(s.tx.onRollback, func() {
		s.tr.Delete(it)
	})

	return nil
}

func (s *storeTx) Get(k []byte) ([]byte, error) {
	select {
	case <-s.tx.ctx.Done():
		return nil, s.tx.ctx.Err()
	default:
	}

	it := s.tr.Get(&item{k: k})

	if it == nil {
		return nil, errors.Wrap(engine.ErrKeyNotFound)
	}

	i := it.(*item)
	// don't return items that have been deleted during
	// this transaction.
	if i.deleted {
		return nil, errors.Wrap(engine.ErrKeyNotFound)
	}

	return it.(*item).v, nil
}

// Delete marks k for deletion. The item will be actually
// deleted during the commit phase of the current transaction.
// The deletion is delayed to avoid a rebalancing of the tree
// every time we remove an item from it,
// which causes iterators to behave incorrectly when looping
// and deleting at the same time.
func (s *storeTx) Delete(k []byte) error {
	select {
	case <-s.tx.ctx.Done():
		return s.tx.ctx.Err()
	default:
	}

	if !s.tx.writable {
		return errors.Wrap(engine.ErrTransactionReadOnly)
	}

	it := s.tr.Get(&item{k: k})
	if it == nil {
		return errors.Wrap(engine.ErrKeyNotFound)
	}

	i := it.(*item)
	// items that have been deleted during
	// this transaction must be ignored.
	if i.deleted {
		return errors.Wrap(engine.ErrKeyNotFound)
	}

	// set the deleted flag to true.
	// this makes the item invisible during this
	// transaction without actually deleting it
	// from the tree.
	// once the transaction is commited, actually
	// remove it from the tree.
	i.deleted = true

	// on rollback set the deleted flag to false.
	s.tx.onRollback = append(s.tx.onRollback, func() {
		i.deleted = false
	})

	// on commit, remove the item from the tree.
	s.tx.onCommit = append(s.tx.onCommit, func() {
		if i.deleted {
			s.tr.Delete(i)
		}
	})
	return nil
}

// Truncate replaces the current tree by a new
// one. The current tree will be garbage collected
// once the transaction is commited.
func (s *storeTx) Truncate() error {
	select {
	case <-s.tx.ctx.Done():
		return s.tx.ctx.Err()
	default:
	}

	if !s.tx.writable {
		return errors.Wrap(engine.ErrTransactionReadOnly)
	}

	old := s.tr
	s.tr = btree.New(btreeDegree)
	s.tx.ng.stores[s.name] = s.tr

	// on rollback replace the new tree by the old one.
	s.tx.onRollback = append(s.tx.onRollback, func() {
		s.tr = old
		s.tx.ng.stores[s.name] = old
	})

	return nil
}

// Iterator creates an iterator with the given options.
func (s *storeTx) Iterator(opts engine.IteratorOptions) engine.Iterator {
	return &iterator{
		ctx:     s.tx.ctx,
		tx:      s.tx,
		tr:      s.tr,
		buf:     make([]*item, 0, itBufSize),
		reverse: opts.Reverse,
	}
}

const itBufSize = 64

// iterator iterates over the btree in batches.

type iterator struct {
	ctx     context.Context
	tx      *transaction
	reverse bool
	tr      *btree.BTree

	// buf stores a batch of itBufSize items
	buf []*item

	// cursor represents the current item in the batch
	cursor int

	// seekBuf is used to avoid reallocating an item everytime
	// we need to seek in the tree
	seekBuf item

	// if an error occurs, it is stored in err
	// and Valid returns false
	err error
}

// Seek seeks the pivot and reads a batch of items from the tree.
func (it *iterator) Seek(pivot []byte) {
	// reset the buffer and cursor
	it.buf = it.buf[:0]
	it.cursor = 0

	// build the tree iterator so that it reads at most
	// itBufSize items
	var count int
	iter := btree.ItemIterator(func(i btree.Item) bool {
		it.buf = append(it.buf, i.(*item))
		count++
		return count < itBufSize
	})

	// run the right
	it.seekBuf.k = pivot
	if it.reverse {
		if len(pivot) == 0 {
			it.tr.Descend(iter)
		} else {
			it.tr.DescendLessOrEqual(&it.seekBuf, iter)
		}
	} else {
		if len(pivot) == 0 {
			it.tr.Ascend(iter)
		} else {
			it.tr.AscendGreaterOrEqual(&it.seekBuf, iter)
		}
	}
}

func (it *iterator) Valid() bool {
	if it.err != nil {
		return false
	}

	select {
	case <-it.ctx.Done():
		it.err = it.ctx.Err()
	default:
	}

	// since the iterator can be used
	// while deleting items
	// we need to skip all deleted items
	// until we find one that's not deleted
	for it.cursor < len(it.buf) {
		if !it.buf[it.cursor].deleted {
			break
		}

		it.cursor++
	}

	// if we reached the end of the buffer
	// we need to preload another batch
	for it.cursor >= len(it.buf) && len(it.buf) == itBufSize {
		// get the key of the last item of the buffer
		// and preload from that key
		pivot := it.buf[len(it.buf)-1].k
		it.Seek(pivot)

		// the pivot was part of the previous batch
		// but is also part of the new batch, we need to
		// skip it to avoid duplicate iteration
		it.cursor++

		// since the iterator can be used
		// while deleting items
		// we need to skip all deleted items
		// until we find one that's not deleted
		for it.cursor < len(it.buf) {
			if !it.buf[it.cursor].deleted {
				break
			}

			it.cursor++
		}
	}

	return len(it.buf) > 0 && it.cursor < len(it.buf) && it.err == nil
}

func (it *iterator) Next() {
	it.cursor++
}

func (it *iterator) Err() error {
	return it.err
}

func (it *iterator) Item() engine.Item {
	return it.buf[it.cursor]
}

// Close the inner goroutine.
func (it *iterator) Close() error {
	return nil
}
