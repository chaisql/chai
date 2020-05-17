package index

import (
	"bytes"

	"github.com/asdine/genji/document"
	"github.com/asdine/genji/engine"
)

// ListIndex is an implementation that associates a value with a list of keys.
type ListIndex struct {
	tx   engine.Transaction
	name string
}

// Set associates a value with a key. It is possible to associate multiple keys for the same value
// but a key can be associated to only one value.
func (i *ListIndex) Set(val document.Value, key []byte) error {
	st, err := getOrCreateStore(i.tx, val.Type, i.name)
	if err != nil {
		return err
	}

	v, err := EncodeFieldToIndexValue(val)
	if err != nil {
		return err
	}

	buf := make([]byte, 0, len(v)+len(key)+1)
	buf = append(buf, v...)
	buf = append(buf, separator)
	buf = append(buf, key...)

	return st.Put(buf, nil)
}

// Delete all the references to the key from the index.
func (i *ListIndex) Delete(val document.Value, key []byte) error {
	v, err := EncodeFieldToIndexValue(val)
	if err != nil {
		return err
	}

	st, err := getOrCreateStore(i.tx, val.Type, i.name)
	if err != nil {
		return err
	}

	buf := make([]byte, 0, len(v)+len(key)+1)
	buf = append(buf, v...)
	buf = append(buf, separator)
	buf = append(buf, key...)

	return st.Delete(buf)
}

// AscendGreaterOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in increasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is nil, starts from the beginning.
func (i *ListIndex) AscendGreaterOrEqual(pivot *Pivot, fn func(val document.Value, key []byte) error) error {
	// iterate over all stores in order
	if pivot == nil {
		for t := Null; t <= Bytes; t++ {
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
				f, err := decodeIndexValueToField(t, k[:idx])
				if err != nil {
					it.Close()
					return err
				}

				err = fn(f, k[idx+1:])
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

	st, err := getStore(i.tx, NewTypeFromValueType(pivot.Value.Type), i.name)
	if err != nil {
		return err
	}
	if st == nil {
		return nil
	}

	var data []byte
	if !pivot.empty {
		data, err = EncodeFieldToIndexValue(pivot.Value)
		if err != nil {
			return err
		}
	}

	it := st.NewIterator(engine.IteratorConfig{})
	// https://github.com/tinygo-org/tinygo/issues/1033
	defer func() {
		it.Close()
	}()

	for it.Seek(data); it.Valid(); it.Next() {
		item := it.Item()
		k := item.Key()

		idx := bytes.LastIndexByte(k, separator)
		f, err := decodeIndexValueToField(NewTypeFromValueType(pivot.Value.Type), k[:idx])
		if err != nil {
			return err
		}

		err = fn(f, k[idx+1:])
		if err != nil {
			return err
		}
	}

	return nil
}

// DescendLessOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is nil, starts from the end.
func (i *ListIndex) DescendLessOrEqual(pivot *Pivot, fn func(val document.Value, key []byte) error) error {
	// iterate over all stores in order
	if pivot == nil {
		for t := Bytes; t >= Null; t-- {
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
				f, err := decodeIndexValueToField(t, k[:idx])
				if err != nil {
					it.Close()
					return err
				}

				err = fn(f, k[idx+1:])
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

	st, err := getStore(i.tx, NewTypeFromValueType(pivot.Value.Type), i.name)
	if err != nil {
		return err
	}
	if st == nil {
		return nil
	}

	var data []byte
	if !pivot.empty {
		data, err = EncodeFieldToIndexValue(pivot.Value)
		if err != nil {
			return err
		}
	}

	if len(data) > 0 {
		// ensure the pivot is bigger than the requested value so it doesn't get skipped.
		data = append(data, separator, 0xFF)
	}

	it := st.NewIterator(engine.IteratorConfig{Reverse: true})
	// https://github.com/tinygo-org/tinygo/issues/1033
	defer func() {
		it.Close()
	}()

	for it.Seek(data); it.Valid(); it.Next() {
		item := it.Item()
		k := item.Key()

		idx := bytes.LastIndexByte(k, separator)
		f, err := decodeIndexValueToField(NewTypeFromValueType(pivot.Value.Type), k[:idx])
		if err != nil {
			return err
		}

		err = fn(f, k[idx+1:])
		if err != nil {
			return err
		}
	}

	return nil
}

// Truncate deletes all the index data.
func (i *ListIndex) Truncate() error {
	err := dropStore(i.tx, Float, i.name)
	if err != nil {
		return err
	}

	err = dropStore(i.tx, Bytes, i.name)
	if err != nil {
		return err
	}

	return dropStore(i.tx, Bool, i.name)
}
