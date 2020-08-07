package index

import (
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
func (i *UniqueIndex) Set(v document.Value, k []byte) error {
	enc, err := key.EncodeValue(v)
	if err != nil {
		return err
	}

	st, err := getOrCreateStore(i.tx, v.Type, i.name)
	if err != nil {
		return err
	}

	buf := make([]byte, 0, len(enc)+2)
	buf = append(buf, uint8(v.Type))
	buf = append(buf, separator)
	buf = append(buf, enc...)

	_, err = st.Get(buf)
	if err == nil {
		return ErrDuplicate
	}
	if err != engine.ErrKeyNotFound {
		return err
	}

	return st.Put(buf, k)
}

// Delete all the references to the key from the index.
func (i *UniqueIndex) Delete(v document.Value, k []byte) error {
	enc, err := key.EncodeValue(v)
	if err != nil {
		return err
	}

	st, err := getOrCreateStore(i.tx, v.Type, i.name)
	if err != nil {
		return err
	}

	buf := make([]byte, 0, len(enc)+2)
	buf = append(buf, uint8(v.Type))
	buf = append(buf, separator)
	buf = append(buf, enc...)

	return st.Delete(buf)
}

// AscendGreaterOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in increasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is nil, starts from the beginning.
func (i *UniqueIndex) AscendGreaterOrEqual(pivot *Pivot, fn func(val, key []byte) error) error {
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

			var v []byte
			for it.Seek(nil); it.Valid(); it.Next() {
				item := it.Item()
				v, err = item.ValueCopy(v[:0])
				if err != nil {
					it.Close()
					return err
				}
				err = fn(item.Key()[2:], v)
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

	seek := make([]byte, 0, len(pivot.EncodedValue)+2)
	seek = append(seek, uint8(pivot.Type))
	seek = append(seek, separator)
	seek = append(seek, pivot.EncodedValue...)

	it := st.NewIterator(engine.IteratorConfig{})
	defer it.Close()

	var pk []byte
	for it.Seek(seek); it.Valid(); it.Next() {
		item := it.Item()

		pk, err = item.ValueCopy(pk[:0])
		if err != nil {
			return err
		}

		err = fn(item.Key()[2:], pk)
		if err != nil {
			return err
		}
	}

	return nil
}

// DescendLessOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is nil, starts from the end.
func (i *UniqueIndex) DescendLessOrEqual(pivot *Pivot, fn func(val, key []byte) error) error {
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

			var v []byte
			for it.Seek(nil); it.Valid(); it.Next() {
				item := it.Item()

				v, err = item.ValueCopy(v[:0])
				if err != nil {
					it.Close()
					return err
				}

				err = fn(item.Key()[2:], v)
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

	seek := make([]byte, 0, len(pivot.EncodedValue)+3)
	seek = append(seek, uint8(pivot.Type))
	seek = append(seek, separator)
	seek = append(seek, pivot.EncodedValue...)
	seek = append(seek, 0xFF)

	it := st.NewIterator(engine.IteratorConfig{Reverse: true})
	defer it.Close()

	var pk []byte
	for it.Seek(seek); it.Valid(); it.Next() {
		item := it.Item()

		pk, err = item.ValueCopy(pk[:0])
		if err != nil {
			return err
		}

		err = fn(item.Key()[2:], pk)
		if err != nil {
			return err
		}
	}

	return nil
}

// Truncate deletes all the index data.
func (i *UniqueIndex) Truncate() error {
	for t := document.NullValue; t <= document.BlobValue; t++ {
		err := dropStore(i.tx, t, i.name)
		if err != nil {
			return err
		}
	}

	return nil
}
