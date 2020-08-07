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
func (i *ListIndex) Set(v document.Value, k []byte) error {
	st, err := getOrCreateStore(i.tx, v.Type, i.name)
	if err != nil {
		return err
	}

	enc, err := key.EncodeValue(v)
	if err != nil {
		return err
	}

	buf := make([]byte, 0, len(enc)+len(k)+1)
	buf = append(buf, enc...)
	buf = append(buf, separator)
	buf = append(buf, k...)

	return st.Put(buf, nil)
}

// Delete all the references to the key from the index.
func (i *ListIndex) Delete(v document.Value, k []byte) error {
	enc, err := key.EncodeValue(v)
	if err != nil {
		return err
	}

	st, err := getOrCreateStore(i.tx, v.Type, i.name)
	if err != nil {
		return err
	}

	buf := make([]byte, 0, len(enc)+len(k)+1)
	buf = append(buf, enc...)
	buf = append(buf, separator)
	buf = append(buf, k...)

	return st.Delete(buf)
}

// AscendGreaterOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in increasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is nil, starts from the beginning.
func (i *ListIndex) AscendGreaterOrEqual(pivot *Pivot, fn func(val, key []byte) error) error {
	// iterate over all stores in order
	if pivot == nil {
		for t := document.NullValue; t <= document.BlobValue; t++ {
			st, err := getStore(i.tx, t, i.name)
			if err != nil {
				return err
			}
			if st == nil {
				continue
			}

			it := st.NewIterator(engine.IteratorConfig{})
			for it.Seek(nil); it.Valid(); it.Next() {
				item := it.Item()
				k := item.Key()
				idx := bytes.LastIndexByte(k, separator)
				err = fn(k[:idx], k[idx+1:])
				if err != nil {
					it.Close()
					return err
				}
			}
			err = it.Close()
			if err != nil {
				return err
			}
		}

		return nil
	}

	st, err := getStore(i.tx, pivot.Type, i.name)
	if err != nil {
		return err
	}
	if st == nil {
		return nil
	}

	it := st.NewIterator(engine.IteratorConfig{})
	defer it.Close()

	for it.Seek(pivot.EncodedValue); it.Valid(); it.Next() {
		item := it.Item()
		k := item.Key()

		idx := bytes.LastIndexByte(k, separator)
		err = fn(k[:idx], k[idx+1:])
		if err != nil {
			return err
		}
	}

	return nil
}

// DescendLessOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is nil, starts from the end.
func (i *ListIndex) DescendLessOrEqual(pivot *Pivot, fn func(val, key []byte) error) error {
	// iterate over all stores in order
	if pivot == nil {
		for t := document.BlobValue; t >= document.NullValue; t-- {
			st, err := getStore(i.tx, t, i.name)
			if err != nil {
				return err
			}
			if st == nil {
				continue
			}

			it := st.NewIterator(engine.IteratorConfig{Reverse: true})

			for it.Seek(nil); it.Valid(); it.Next() {
				item := it.Item()
				k := item.Key()

				idx := bytes.LastIndexByte(k, separator)
				err = fn(k[:idx], k[idx+1:])
				if err != nil {
					it.Close()
					return err
				}
			}

			err = it.Close()
			if err != nil {
				return err
			}

		}

		return nil
	}

	st, err := getStore(i.tx, pivot.Type, i.name)
	if err != nil {
		return err
	}
	if st == nil {
		return nil
	}

	if len(pivot.EncodedValue) > 0 {
		// ensure the pivot is bigger than the requested value so it doesn't get skipped.
		pivot.EncodedValue = append(pivot.EncodedValue, separator, 0xFF)
	}

	it := st.NewIterator(engine.IteratorConfig{Reverse: true})
	defer it.Close()

	for it.Seek(pivot.EncodedValue); it.Valid(); it.Next() {
		item := it.Item()
		k := item.Key()

		idx := bytes.LastIndexByte(k, separator)
		err = fn(k[:idx], k[idx+1:])
		if err != nil {
			return err
		}
	}

	return nil
}

// Truncate deletes all the index data.
func (i *ListIndex) Truncate() error {
	for t := document.NullValue; t <= document.BlobValue; t++ {
		err := dropStore(i.tx, t, i.name)
		if err != nil {
			return err
		}
	}

	return nil
}
