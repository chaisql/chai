package memoryengine

import (
	"bytes"
	"context"
	"errors"

	"github.com/genjidb/genji/engine"
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
	copy(buf, i.v)
	return buf, nil
}

func (i *item) Less(than btree.Item) bool {
	return bytes.Compare(i.k, than.(*item).k) < 0
}

// storeTx implements an engine.Store.
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
	it := s.tr.Get(&item{k: k})

	if it == nil {
		return nil, engine.ErrKeyNotFound
	}

	i := it.(*item)
	// don't return items that have been deleted during
	// this transaction.
	if i.deleted {
		return nil, engine.ErrKeyNotFound
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
	if !s.tx.writable {
		return engine.ErrTransactionReadOnly
	}

	it := s.tr.Get(&item{k: k})
	if it == nil {
		return engine.ErrKeyNotFound
	}

	i := it.(*item)
	// items that have been deleted during
	// this transaction must be ignored.
	if i.deleted {
		return engine.ErrKeyNotFound
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
		s.tr.Delete(i)
	})
	return nil
}

// Truncate replaces the current tree by a new
// one. The current tree will be garbage collected
// once the transaction is commited.
func (s *storeTx) Truncate() error {
	if !s.tx.writable {
		return engine.ErrTransactionReadOnly
	}

	old := s.tr
	s.tr = btree.New(btreeDegree)

	// on rollback replace the new tree by the old one.
	s.tx.onRollback = append(s.tx.onRollback, func() {
		s.tr = old
	})

	return nil
}

func (s *storeTx) NewIterator(cfg engine.IteratorConfig) engine.Iterator {
	return &iterator{
		tx:      s.tx,
		tr:      s.tr,
		reverse: cfg.Reverse,
		ch:      make(chan *item),
		closed:  make(chan struct{}),
	}
}

// iterator uses a goroutine to read from the tree on demand.
type iterator struct {
	tx      *transaction
	reverse bool
	tr      *btree.BTree
	item    *item // current item
	ch      chan *item
	closed  chan struct{} // closed by the goroutine when it's shutdown
	ctx     context.Context
	cancel  func()
}

func (it *iterator) Seek(pivot []byte) {
	// make sure any opened goroutine
	// is closed before creating a new one
	if it.cancel != nil {
		it.cancel()
		it.ch = make(chan *item)
		<-it.closed
		it.closed = make(chan struct{})
	}

	it.ctx, it.cancel = context.WithCancel(context.Background())

	it.runIterator(pivot)

	it.Next()
}

// runIterator creates a goroutine that reads from the tree.
// Once the goroutine is done reading or if the context is canceled,
// both ch and closed channels will be closed.
func (it *iterator) runIterator(pivot []byte) {
	it.tx.wg.Add(1)

	go func(ctx context.Context, ch chan *item, tr *btree.BTree) {
		defer it.tx.wg.Done()
		defer close(ch)
		defer close(it.closed)

		iter := btree.ItemIterator(func(i btree.Item) bool {
			select {
			case <-ctx.Done():
				return false
			default:
			}

			itm := i.(*item)
			if itm.deleted {
				return true
			}

			select {
			case <-ctx.Done():
				return false
			case ch <- itm:
				return true
			}
		})

		if it.reverse {
			if len(pivot) == 0 {
				tr.Descend(iter)
			} else {
				tr.DescendLessOrEqual(&item{k: pivot}, iter)
			}
		} else {
			if len(pivot) == 0 {
				tr.Ascend(iter)
			} else {
				tr.AscendGreaterOrEqual(&item{k: pivot}, iter)
			}
		}
	}(it.ctx, it.ch, it.tr)
}

func (it *iterator) Valid() bool {
	return it.item != nil
}

// Read the next item from the goroutine
func (it *iterator) Next() {
	it.item = <-it.ch
}

func (it *iterator) Item() engine.Item {
	return it.item
}

// Close the inner goroutine.
func (it *iterator) Close() error {
	if it.cancel != nil {
		it.cancel()
		it.cancel = nil
		<-it.closed
	}

	return nil
}
