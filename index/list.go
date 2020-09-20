package index

import (
	"bytes"
	"errors"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/key"
)

// ListIndex is an implementation that associates a value with a list of keys.
type ListIndex struct {
	tx        engine.Transaction
	name      string
	storeName []byte
}

// Set associates a value with a key. It is possible to associate multiple keys for the same value
// but a key can be associated to only one value.
func (idx *ListIndex) Set(v document.Value, k []byte) error {
	var err error

	if len(k) == 0 {
		return errors.New("cannot index value without a key")
	}

	st, err := getOrCreateStore(idx.tx, idx.storeName)
	if err != nil {
		return nil
	}

	buf, err := key.AppendValue(nil, v)
	if err != nil {
		return err
	}

	lookupKey := append(buf, 0)

	_, err = st.Get(lookupKey)
	switch err {
	case nil:
		seq, err := st.NextSequence()
		if err != nil {
			return err
		}
		buf = key.AppendInt64(buf, int64(seq))
		buf = append(buf, 1)
	case engine.ErrKeyNotFound:
		buf = lookupKey
	default:
		return err
	}

	return st.Put(buf, k)
}

// Delete all the references to the key from the index.
func (idx *ListIndex) Delete(v document.Value, k []byte) error {
	st, err := getOrCreateStore(idx.tx, idx.storeName)
	if err != nil {
		return nil
	}

	var toDelete []byte
	var buf []byte
	err = iterate(st, v, false, func(encodedValue []byte, item engine.Item) error {
		buf, err = item.ValueCopy(buf)
		if err != nil {
			return err
		}
		if bytes.Equal(buf, k) {
			toDelete = item.Key()
			return errStop
		}

		return nil
	})
	if err != errStop && err != nil {
		return err
	}

	if toDelete != nil {
		return st.Delete(toDelete)
	}

	return engine.ErrKeyNotFound
}

// AscendGreaterOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in increasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is nil, starts from the beginning.
func (idx *ListIndex) AscendGreaterOrEqual(pivot document.Value, fn func(val, key []byte, isEqual bool) error) error {
	return idx.iterateOnStore(pivot, false, fn)
}

// DescendLessOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is nil, starts from the end.
func (idx *ListIndex) DescendLessOrEqual(pivot document.Value, fn func(val, key []byte, isEqual bool) error) error {
	return idx.iterateOnStore(pivot, true, fn)
}

func (idx *ListIndex) iterateOnStore(pivot document.Value, reverse bool, fn func(val, key []byte, isEqual bool) error) error {
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
		if k[len(k)-1] == 0 {
			k = k[:len(k)-1]
		} else {
			k = k[:len(k)-9]
		}

		buf, err = item.ValueCopy(buf[:0])
		if err != nil {
			return err
		}

		return fn(k, buf, bytes.Equal(k, encodedValue))
	})
}

// Truncate deletes all the index data.
func (idx *ListIndex) Truncate() error {
	err := idx.tx.DropStore(idx.storeName)
	if err != nil && err != engine.ErrStoreNotFound {
		return err
	}

	return nil
}
