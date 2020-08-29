package index

import (
	"bytes"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/key"
)

// ListIndex is an implementation that associates a value with a list of keys.
type ListIndex struct {
	tx   engine.Transaction
	name string
}

// Set associates a value with a key. It is possible to associate multiple keys for the same value
// but a key can be associated to only one value.
func (idx *ListIndex) Set(v document.Value, k []byte) error {
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

	buf := key.AppendValue(nil, v)
	seq, err := st.NextSequence()
	if err != nil {
		return err
	}
	buf = key.AppendUint64(buf, seq)

	return st.Put(buf, k)
}

// Delete all the references to the key from the index.
func (idx *ListIndex) Delete(v document.Value, k []byte) error {
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

	seek := key.AppendValue(nil, v)

	it := st.NewIterator(engine.IteratorConfig{})
	defer it.Close()

	var buf []byte
	var toDelete []byte
	for it.Seek(seek); it.Valid(); it.Next() {
		item := it.Item()
		buf, err = item.ValueCopy(buf)
		if err != nil {
			return err
		}
		if bytes.Equal(buf, k) {
			toDelete = item.Key()
			break
		}
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
func (idx *ListIndex) DescendLessOrEqual(pivot document.Value, fn func(val, key []byte, isEqual bool) error) error {
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

func (idx *ListIndex) iterateOnStore(pivot document.Value, reverse bool, fn func(val, key []byte, isEqual bool) error) error {
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

	var buf []byte
	for it.Seek(seek); it.Valid(); it.Next() {
		item := it.Item()
		v := item.Key()
		idx := len(v) - 8

		buf, err = item.ValueCopy(buf)
		if err != nil {
			return err
		}

		err = fn(v[:idx], buf, bytes.Equal(v[:idx], enc))
		if err != nil {
			return err
		}
	}

	return nil
}

// Truncate deletes all the index data.
func (idx *ListIndex) Truncate() error {
	for t := document.NullValue; t <= document.BlobValue; t++ {
		err := dropStore(idx.tx, t, idx.name)
		if err != nil {
			return err
		}
	}

	return nil
}
