package index

import (
	"bytes"
	"errors"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/key"
)

// UniqueIndex is an implementation that associates a value with a exactly one key.
type UniqueIndex struct {
	tx        engine.Transaction
	name      string
	storeName []byte
}

// Set associates a value with exactly one key.
// If the association already exists, it returns an error.
// It stores integers as doubles.
func (idx *UniqueIndex) Set(v document.Value, k []byte) error {
	var err error

	if len(k) == 0 {
		return errors.New("cannot index value without a key")
	}

	st, err := getOrCreateStore(idx.tx, idx.storeName)
	if err != nil {
		return nil
	}

	enc, err := key.AppendValue(nil, v)
	if err != nil {
		return err
	}

	_, err = st.Get(enc)
	if err == nil {
		return ErrDuplicate
	}
	if err != engine.ErrKeyNotFound {
		return err
	}

	return st.Put(enc, k)
}

// Delete all the references to the key from the index.
func (idx *UniqueIndex) Delete(v document.Value, k []byte) error {
	st, err := getOrCreateStore(idx.tx, idx.storeName)
	if err != nil {
		return nil
	}

	enc, err := key.AppendValue(nil, v)
	if err != nil {
		return err
	}

	return st.Delete(enc)
}

// AscendGreaterOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in increasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is nil, starts from the beginning.
func (idx *UniqueIndex) AscendGreaterOrEqual(pivot document.Value, fn func(val, key []byte, isEqual bool) error) error {
	return idx.iterateOnStore(pivot, false, fn)
}

// DescendLessOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is nil, starts from the end.
func (idx *UniqueIndex) DescendLessOrEqual(pivot document.Value, fn func(val, key []byte, isEqual bool) error) error {
	return idx.iterateOnStore(pivot, true, fn)
}

func (idx *UniqueIndex) iterateOnStore(pivot document.Value, reverse bool, fn func(val, key []byte, isEqual bool) error) error {
	var buf []byte
	st, err := idx.tx.GetStore(idx.storeName)
	if err != nil && err != engine.ErrStoreNotFound {
		return err
	}
	if st == nil {
		return nil
	}

	return iterate(st, pivot, reverse, func(encodedValue []byte, item engine.Item) error {
		var err error

		k := item.Key()

		buf, err = item.ValueCopy(buf[:0])
		if err != nil {
			return err
		}

		return fn(k, buf, bytes.Equal(k, encodedValue))
	})
}

// Truncate deletes all the index data.
func (idx *UniqueIndex) Truncate() error {
	err := idx.tx.DropStore(idx.storeName)
	if err != nil && err != engine.ErrStoreNotFound {
		return err
	}

	return nil
}
