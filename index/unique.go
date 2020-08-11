package index

import (
	"bytes"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/key"
)

// UniqueIndex is an implementation that associates a value with a exactly one key.
type UniqueIndex struct {
	tx   engine.Transaction
	name string
}

// Set associates a value with exactly one key.
// If the association already exists, it returns an error.
// It stores integers as doubles.
func (idx *UniqueIndex) Set(v document.Value, k []byte) error {
	var err error

	if v.Type == document.IntegerValue {
		v, err = v.CastAsDouble()
		if err != nil {
			return err
		}
	}

	st, err := getOrCreateStore(idx.tx, v.Type, idx.name)
	if err != nil {
		return err
	}

	enc := key.AppendValue(nil, v)

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
	var err error

	if v.Type == document.IntegerValue {
		v, err = v.CastAsDouble()
		if err != nil {
			return err
		}
	}

	st, err := getOrCreateStore(idx.tx, v.Type, idx.name)
	if err != nil {
		return err
	}

	enc := key.AppendValue(nil, v)

	return st.Delete(enc)
}

// AscendGreaterOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in increasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is nil, starts from the beginning.
func (idx *UniqueIndex) AscendGreaterOrEqual(pivot document.Value, fn func(val, key []byte, isEqual bool) error) error {
	// iterate over all stores in order
	if pivot.Type == 0 {
		for i := 0; i < len(valueTypes); i++ {
			err := idx.iterateOnStore(document.Value{Type: valueTypes[i]}, false, fn)
			if err != nil {
				return err
			}
		}

		return nil
	}

	return idx.iterateOnStore(pivot, false, fn)
}

// DescendLessOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is nil, starts from the end.
func (idx *UniqueIndex) DescendLessOrEqual(pivot document.Value, fn func(val, key []byte, isEqual bool) error) error {
	// iterate over all stores in order
	if pivot.Type == 0 {
		for i := len(valueTypes) - 1; i >= 0; i-- {
			err := idx.iterateOnStore(document.Value{Type: valueTypes[i]}, true, fn)
			if err != nil {
				return err
			}
		}

		return nil
	}

	return idx.iterateOnStore(pivot, true, fn)
}

func (idx *UniqueIndex) iterateOnStore(pivot document.Value, reverse bool, fn func(val, key []byte, isEqual bool) error) error {
	var err error

	if pivot.Type == document.IntegerValue {
		if pivot.V != nil {
			pivot, err = pivot.CastAsDouble()
			if err != nil {
				return err
			}
		} else {
			pivot.Type = document.DoubleValue
		}
	}

	st, err := getStore(idx.tx, pivot.Type, idx.name)
	if err != nil {
		return err
	}
	if st == nil {
		return nil
	}
	var v []byte

	var seek, enc []byte

	if pivot.V != nil {
		enc = key.AppendValue(nil, pivot)
		seek = enc

		if reverse {
			seek = append(seek, 0xFF)
		}
	}

	it := st.NewIterator(engine.IteratorConfig{Reverse: reverse})
	defer it.Close()

	for it.Seek(seek); it.Valid(); it.Next() {
		item := it.Item()

		v, err = item.ValueCopy(v[:0])
		if err != nil {
			return err
		}

		k := item.Key()
		err = fn(k, v, bytes.Equal(k, enc))
		if err != nil {
			return err
		}
	}

	return nil
}

// Truncate deletes all the index data.
func (idx *UniqueIndex) Truncate() error {
	for t := document.NullValue; t <= document.BlobValue; t++ {
		err := dropStore(idx.tx, t, idx.name)
		if err != nil {
			return err
		}
	}

	return nil
}
