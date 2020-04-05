package index

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/asdine/genji/document"
	"github.com/asdine/genji/document/encoding"
	"github.com/asdine/genji/engine"
)

const (
	// StorePrefix is the prefix used to name the index store.
	StorePrefix = "i" + string(separator)

	separator byte = 0x1E
)

// Type of the index. Values are stored in different sub indexes depending on their types.
// They are automatically converted to one of the following types:
//
// Text and Blob values are stored in Bytes indexes.
// Signed, unsigned integers, and floats are stored in Float indexes.
// Booleans are stores in Bool indexes.
type Type byte

// index value types
const (
	Null Type = iota + 1
	Bool
	Float
	Bytes
)

// NewTypeFromValueType returns the right index type associated with t.
func NewTypeFromValueType(t document.ValueType) Type {
	if t.IsNumber() {
		return Float
	}

	if t == document.TextValue || t == document.BlobValue {
		return Bytes
	}

	if t == document.BoolValue {
		return Bool
	}

	return Null
}

var (
	// ErrDuplicate is returned when a value is already associated with a key
	ErrDuplicate = errors.New("duplicate")
)

// An Index associates encoded values with keys.
// It is sorted by value following the lexicographic order.
type Index interface {
	// Set associates a value with a key.
	Set(val document.Value, key []byte) error

	// Delete all the references to the key from the index.
	Delete(val document.Value, key []byte) error

	// AscendGreaterOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in increasing order and calls the given function for each pair.
	// If the given function returns an error, the iteration stops and returns that error.
	// If the pivot is nil, starts from the beginning.
	AscendGreaterOrEqual(pivot *Pivot, fn func(val document.Value, key []byte) error) error

	// DescendLessOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
	// If the given function returns an error, the iteration stops and returns that error.
	// If the pivot is nil, starts from the end.
	DescendLessOrEqual(pivot *Pivot, fn func(val document.Value, key []byte) error) error

	// Truncate deletes all the index data.
	Truncate() error
}

// NewListIndex creates an index that associates a value with a list of keys.
func NewListIndex(tx engine.Transaction, idxName string) *ListIndex {
	return &ListIndex{
		tx:   tx,
		name: idxName,
	}
}

// NewUniqueIndex creates an index that associates a value with a exactly one key.
func NewUniqueIndex(tx engine.Transaction, idxName string) *UniqueIndex {
	return &UniqueIndex{
		tx:   tx,
		name: idxName,
	}
}

func buildIndexName(name string, t Type) string {
	var b strings.Builder
	b.WriteString(StorePrefix)
	b.WriteString(name)
	b.WriteByte(separator)
	b.WriteByte(byte(t))

	return b.String()
}

// A Pivot is a value that is used to seek for a particular value in an index.
// A Pivot is typed and can only be used to seek for values of the same type.
type Pivot struct {
	Value document.Value
	empty bool
}

// EmptyPivot returns a pivot that starts at the beginning of any indexed values compatible with the given type.
func EmptyPivot(t document.ValueType) *Pivot {
	v := document.NewZeroValue(t)
	return &Pivot{
		Value: v,
		empty: true,
	}
}

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
	defer it.Close()

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
	defer it.Close()

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
	defer it.Close()

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
	defer it.Close()

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

// EncodeFieldToIndexValue returns a byte array that represents the value in such
// a way that can be compared for ordering and indexing
func EncodeFieldToIndexValue(val document.Value) ([]byte, error) {
	if val.V != nil && val.Type.IsNumber() && val.Type != document.Float64Value {
		x, err := val.ConvertToFloat64()
		if err != nil {
			return nil, err
		}

		return encoding.EncodeFloat64(x), nil
	}

	return encoding.EncodeValue(val)
}

func decodeIndexValueToField(t Type, data []byte) (document.Value, error) {
	switch t {
	case Null:
		return document.NewNullValue(), nil
	case Bytes:
		return document.NewBlobValue(data), nil
	case Float:
		f, err := encoding.DecodeFloat64(data)
		return document.NewFloat64Value(f), err
	case Bool:
		b, err := encoding.DecodeBool(data)
		return document.NewBoolValue(b), err
	}

	return document.Value{}, fmt.Errorf("unknown index type %d", t)
}

func getOrCreateStore(tx engine.Transaction, t document.ValueType, name string) (engine.Store, error) {
	idxName := buildIndexName(name, NewTypeFromValueType(t))
	st, err := tx.GetStore(idxName)
	if err == nil {
		return st, nil
	}

	if err != engine.ErrStoreNotFound {
		return nil, err
	}

	err = tx.CreateStore(idxName)
	if err != nil {
		return nil, err
	}

	return tx.GetStore(idxName)
}

func getStore(tx engine.Transaction, t Type, name string) (engine.Store, error) {
	idxName := buildIndexName(name, t)
	st, err := tx.GetStore(idxName)
	if err == nil || err == engine.ErrStoreNotFound {
		return st, nil
	}

	return nil, err
}

func dropStore(tx engine.Transaction, t Type, name string) error {
	idxName := buildIndexName(name, t)
	_, err := tx.GetStore(idxName)
	if err != nil && err != engine.ErrStoreNotFound {
		return err
	}

	if err == engine.ErrStoreNotFound {
		return nil
	}

	return tx.DropStore(idxName)
}
