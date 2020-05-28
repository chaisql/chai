package index

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
)

// UniqueIndex is an implementation that associates a value with a exactly one key.
type UniqueIndex struct {
	tx   engine.Transaction
	name string
}

// Set associates a value with exactly one key.
// If the association already exists, it returns an error.
func (i *UniqueIndex) Set(val document.Value, key []byte) error {
	v, err := EncodeFieldToIndexValue(val)
	if err != nil {
		return err
	}

	st, err := getOrCreateStore(i.tx, val.Type, i.name)
	if err != nil {
		return err
	}

	buf := make([]byte, 0, len(v)+2)
	buf = append(buf, uint8(NewTypeFromValueType(val.Type)))
	buf = append(buf, separator)
	buf = append(buf, v...)

	_, err = st.Get(buf)
	if err == nil {
		return ErrDuplicate
	}
	if err != engine.ErrKeyNotFound {
		return err
	}

	return st.Put(buf, key)
}

// Delete all the references to the key from the index.
func (i *UniqueIndex) Delete(val document.Value, key []byte) error {
	v, err := EncodeFieldToIndexValue(val)
	if err != nil {
		return err
	}

	st, err := getOrCreateStore(i.tx, val.Type, i.name)
	if err != nil {
		return err
	}

	buf := make([]byte, 0, len(v)+2)
	buf = append(buf, uint8(NewTypeFromValueType(val.Type)))
	buf = append(buf, separator)
	buf = append(buf, v...)

	return st.Delete(buf)
}

// AscendGreaterOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in increasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is nil, starts from the beginning.
func (i *UniqueIndex) AscendGreaterOrEqual(pivot *Pivot, fn func(val document.Value, key []byte) error) error {
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

			var v []byte
			for it.Seek(nil); it.Valid(); it.Next() {
				item := it.Item()
				f, err := decodeIndexValueToField(t, item.Key()[2:])
				if err != nil {
					it.Close()
					return err
				}

				v, err = item.ValueCopy(v[:0])
				if err != nil {
					it.Close()
					return err
				}
				err = fn(f, v)
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

	seek := make([]byte, 0, len(data)+2)
	seek = append(seek, uint8(NewTypeFromValueType(pivot.Value.Type)))
	seek = append(seek, separator)
	seek = append(seek, data...)

	it := st.NewIterator(engine.IteratorConfig{})
	// https://github.com/tinygo-org/tinygo/issues/1033
	defer func() {
		it.Close()
	}()

	var pk []byte
	for it.Seek(seek); it.Valid(); it.Next() {
		item := it.Item()

		pk, err = item.ValueCopy(pk[:0])
		if err != nil {
			return err
		}

		f, err := decodeIndexValueToField(NewTypeFromValueType(pivot.Value.Type), item.Key()[2:])
		if err != nil {
			return err
		}

		err = fn(f, pk)
		if err != nil {
			return err
		}
	}

	return nil
}

// DescendLessOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is nil, starts from the end.
func (i *UniqueIndex) DescendLessOrEqual(pivot *Pivot, fn func(val document.Value, key []byte) error) error {
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

			var v []byte
			for it.Seek(nil); it.Valid(); it.Next() {
				item := it.Item()

				f, err := decodeIndexValueToField(t, item.Key()[2:])
				if err != nil {
					it.Close()
					return err
				}

				v, err = item.ValueCopy(v[:0])
				if err != nil {
					it.Close()
					return err
				}

				err = fn(f, v)
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

	seek := make([]byte, 0, len(data)+3)
	seek = append(seek, uint8(NewTypeFromValueType(pivot.Value.Type)))
	seek = append(seek, separator)
	seek = append(seek, data...)
	seek = append(seek, 0xFF)

	it := st.NewIterator(engine.IteratorConfig{Reverse: true})
	// https://github.com/tinygo-org/tinygo/issues/1033
	defer func() {
		it.Close()
	}()

	var pk []byte
	for it.Seek(seek); it.Valid(); it.Next() {
		item := it.Item()

		pk, err = item.ValueCopy(pk[:0])
		if err != nil {
			return err
		}

		f, err := decodeIndexValueToField(NewTypeFromValueType(pivot.Value.Type), item.Key()[2:])
		if err != nil {
			return err
		}

		err = fn(f, pk)
		if err != nil {
			return err
		}
	}

	return nil
}

// Truncate deletes all the index data.
func (i *UniqueIndex) Truncate() error {
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
