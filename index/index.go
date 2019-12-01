package index

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/asdine/genji/document"
	"github.com/asdine/genji/engine"
)

// Prefixes and separators used to name the index stores.
const (
	IndexPrefix      = "i"
	Separator   byte = 0x1E
)

// Type of the index. Values are stored in different sub indexes depending on their types.
// They are automatically converted to one of the following types:
//
// Strings and Bytes values are stored in Bytes indexes.
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

	if t == document.StringValue || t == document.BytesValue {
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
	AscendGreaterOrEqual(pivot *document.Value, fn func(val document.Value, key []byte) error) error

	// DescendLessOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
	// If the given function returns an error, the iteration stops and returns that error.
	// If the pivot is nil, starts from the end.
	DescendLessOrEqual(pivot *document.Value, fn func(val document.Value, key []byte) error) error

	// Truncate deletes all the index data.
	Truncate() error
}

func buildIndexName(name string, t Type) string {
	var b strings.Builder
	b.WriteString(IndexPrefix)
	b.WriteByte(Separator)
	b.WriteString(name)
	b.WriteByte(Separator)
	b.WriteByte(byte(t))

	return b.String()
}

// Options of the index.
type Options struct {
	// If set to true, values will be associated with at most one key. False by default.
	Unique bool

	IndexName string
	TableName string
	FieldName string
}

// EmptyPivot returns a pivot that starts at the beginning of any indexed values compatible with the given type.
func EmptyPivot(t document.ValueType) *document.Value {
	return &document.Value{Type: t}
}

// New creates an index with the given store and options.
func New(tx engine.Transaction, opts Options) Index {
	if opts.Unique {
		return &uniqueIndex{
			tx:   tx,
			opts: opts,
		}
	}

	return &listIndex{
		tx:   tx,
		opts: opts,
	}
}

// listIndex is an implementation that associates a value with a list of keys.
type listIndex struct {
	tx   engine.Transaction
	opts Options
}

// Set associates a value with a key. It is possible to associate multiple keys for the same value
// but a key can be associated to only one value.
func (i *listIndex) Set(val document.Value, key []byte) error {
	st, err := getOrCreateStore(i.tx, val.Type, i.opts)
	if err != nil {
		return err
	}

	v, err := encodeFieldToIndexValue(&val)
	if err != nil {
		return err
	}

	buf := make([]byte, 0, len(v)+len(key)+1)
	buf = append(buf, v...)
	buf = append(buf, Separator)
	buf = append(buf, key...)

	return st.Put(buf, nil)
}

func (i *listIndex) Delete(val document.Value, key []byte) error {
	v, err := encodeFieldToIndexValue(&val)
	if err != nil {
		return err
	}

	st, err := getOrCreateStore(i.tx, val.Type, i.opts)
	if err != nil {
		return err
	}

	buf := make([]byte, 0, len(v)+len(key)+1)
	buf = append(buf, v...)
	buf = append(buf, Separator)
	buf = append(buf, key...)

	return st.Delete(buf)
}

func (i *listIndex) AscendGreaterOrEqual(pivot *document.Value, fn func(val document.Value, key []byte) error) error {
	// iterate over all stores in order
	if pivot == nil {
		for t := Null; t <= Bytes; t++ {
			st, err := getStore(i.tx, t, i.opts)
			if err != nil {
				return err
			}
			if st == nil {
				continue
			}

			err = st.AscendGreaterOrEqual(nil, func(k, v []byte) error {
				idx := bytes.LastIndexByte(k, Separator)
				f, err := decodeIndexValueToField(t, k[:idx])
				if err != nil {
					return err
				}

				return fn(f, k[idx+1:])
			})
			if err != nil {
				return err
			}
		}

		return nil
	}

	st, err := getStore(i.tx, NewTypeFromValueType(pivot.Type), i.opts)
	if err != nil {
		return err
	}
	if st == nil {
		return nil
	}

	v, err := encodeFieldToIndexValue(pivot)
	if err != nil {
		return err
	}

	return st.AscendGreaterOrEqual(v, func(k, v []byte) error {
		idx := bytes.LastIndexByte(k, Separator)
		f, err := decodeIndexValueToField(NewTypeFromValueType(pivot.Type), k[:idx])
		if err != nil {
			return err
		}

		return fn(f, k[idx+1:])
	})
}

func (i *listIndex) DescendLessOrEqual(pivot *document.Value, fn func(val document.Value, key []byte) error) error {
	// iterate over all stores in order
	if pivot == nil {
		for t := Bytes; t >= Null; t-- {
			st, err := getStore(i.tx, t, i.opts)
			if err != nil {
				return err
			}
			if st == nil {
				continue
			}

			err = st.DescendLessOrEqual(nil, func(k, v []byte) error {
				idx := bytes.LastIndexByte(k, Separator)
				f, err := decodeIndexValueToField(t, k[:idx])
				if err != nil {
					return err
				}

				return fn(f, k[idx+1:])
			})
			if err != nil {
				return err
			}
		}

		return nil
	}

	st, err := getStore(i.tx, NewTypeFromValueType(pivot.Type), i.opts)
	if err != nil {
		return err
	}
	if st == nil {
		return nil
	}

	v, err := encodeFieldToIndexValue(pivot)
	if err != nil {
		return err
	}

	if len(v) > 0 {
		// ensure the pivot is bigger than the requested value so it doesn't get skipped.
		v = append(v, Separator, 0xFF)
	}

	return st.DescendLessOrEqual(v, func(k, v []byte) error {
		idx := bytes.LastIndexByte(k, Separator)
		f, err := decodeIndexValueToField(NewTypeFromValueType(pivot.Type), k[:idx])
		if err != nil {
			return err
		}

		return fn(f, k[idx+1:])
	})
}

func (i *listIndex) Truncate() error {
	err := dropStore(i.tx, Float, i.opts)
	if err != nil {
		return err
	}

	err = dropStore(i.tx, Bytes, i.opts)
	if err != nil {
		return err
	}

	return dropStore(i.tx, Bool, i.opts)
}

// uniqueIndex is an implementation that associates a value with a exactly one key.
type uniqueIndex struct {
	tx   engine.Transaction
	opts Options
}

// Set associates a value with exactly one key.
// If the association already exists, it returns an error.
func (i *uniqueIndex) Set(val document.Value, key []byte) error {
	v, err := encodeFieldToIndexValue(&val)
	if err != nil {
		return err
	}

	st, err := getOrCreateStore(i.tx, val.Type, i.opts)
	if err != nil {
		return err
	}

	buf := make([]byte, 0, len(v)+2)
	buf = append(buf, uint8(NewTypeFromValueType(val.Type)))
	buf = append(buf, Separator)
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

func (i *uniqueIndex) Delete(val document.Value, key []byte) error {
	v, err := encodeFieldToIndexValue(&val)
	if err != nil {
		return err
	}

	st, err := getOrCreateStore(i.tx, val.Type, i.opts)
	if err != nil {
		return err
	}

	buf := make([]byte, 0, len(v)+2)
	buf = append(buf, uint8(NewTypeFromValueType(val.Type)))
	buf = append(buf, Separator)
	buf = append(buf, v...)

	return st.Delete(buf)
}

func (i *uniqueIndex) AscendGreaterOrEqual(pivot *document.Value, fn func(val document.Value, key []byte) error) error {
	// iterate over all stores in order
	if pivot == nil {
		for t := Null; t <= Bytes; t++ {
			st, err := getStore(i.tx, t, i.opts)
			if err != nil {
				return err
			}
			if st == nil {
				continue
			}

			err = st.AscendGreaterOrEqual(nil, func(k, v []byte) error {
				f, err := decodeIndexValueToField(t, k[2:])
				if err != nil {
					return err
				}

				return fn(f, v)
			})
			if err != nil {
				return err
			}
		}

		return nil
	}

	st, err := getStore(i.tx, NewTypeFromValueType(pivot.Type), i.opts)
	if err != nil {
		return err
	}
	if st == nil {
		return nil
	}

	v, err := encodeFieldToIndexValue(pivot)
	if err != nil {
		return err
	}

	buf := make([]byte, 0, len(v)+2)
	buf = append(buf, uint8(NewTypeFromValueType(pivot.Type)))
	buf = append(buf, Separator)
	buf = append(buf, v...)

	return st.AscendGreaterOrEqual(buf, func(vv []byte, key []byte) error {
		f, err := decodeIndexValueToField(NewTypeFromValueType(pivot.Type), vv[2:])
		if err != nil {
			return err
		}

		return fn(f, key)
	})
}

func (i *uniqueIndex) DescendLessOrEqual(pivot *document.Value, fn func(val document.Value, key []byte) error) error {
	// iterate over all stores in order
	if pivot == nil {
		for t := Bytes; t >= Null; t-- {
			st, err := getStore(i.tx, t, i.opts)
			if err != nil {
				return err
			}
			if st == nil {
				continue
			}

			err = st.DescendLessOrEqual(nil, func(k, v []byte) error {
				f, err := decodeIndexValueToField(t, k[2:])
				if err != nil {
					return err
				}

				return fn(f, v)
			})
			if err != nil {
				return err
			}
		}

		return nil
	}

	st, err := getStore(i.tx, NewTypeFromValueType(pivot.Type), i.opts)
	if err != nil {
		return err
	}
	if st == nil {
		return nil
	}

	v, err := encodeFieldToIndexValue(pivot)
	if err != nil {
		return err
	}

	buf := make([]byte, 0, len(v)+3)
	buf = append(buf, uint8(NewTypeFromValueType(pivot.Type)))
	buf = append(buf, Separator)
	buf = append(buf, v...)
	buf = append(buf, 0xFF)

	return st.DescendLessOrEqual(buf, func(vv []byte, key []byte) error {
		f, err := decodeIndexValueToField(NewTypeFromValueType(pivot.Type), vv[2:])
		if err != nil {
			return err
		}

		return fn(f, key)
	})
}

func (i *uniqueIndex) Truncate() error {
	err := dropStore(i.tx, Float, i.opts)
	if err != nil {
		return err
	}

	err = dropStore(i.tx, Bytes, i.opts)
	if err != nil {
		return err
	}

	return dropStore(i.tx, Bool, i.opts)
}

func encodeFieldToIndexValue(val *document.Value) ([]byte, error) {
	if len(val.Data) > 0 && val.Type.IsNumber() && val.Type != document.Float64Value {
		x, err := val.DecodeToFloat64()
		if err != nil {
			return nil, err
		}

		return document.NewFloat64Value(x).Data, nil
	}

	return val.Data, nil
}

func decodeIndexValueToField(t Type, data []byte) (document.Value, error) {
	switch t {
	case Null:
		return document.Value{Type: document.NullValue}, nil
	case Bytes:
		return document.Value{Type: document.BytesValue, Data: data}, nil
	case Float:
		return document.Value{Type: document.Float64Value, Data: data}, nil
	case Bool:
		return document.Value{Type: document.BoolValue, Data: data}, nil
	}

	return document.Value{}, fmt.Errorf("unknown index type %d", t)
}

func getOrCreateStore(tx engine.Transaction, t document.ValueType, opts Options) (engine.Store, error) {
	idxName := buildIndexName(opts.IndexName, NewTypeFromValueType(t))
	st, err := tx.Store(idxName)
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

	return tx.Store(idxName)
}

func getStore(tx engine.Transaction, t Type, opts Options) (engine.Store, error) {
	idxName := buildIndexName(opts.IndexName, t)
	st, err := tx.Store(idxName)
	if err == nil || err == engine.ErrStoreNotFound {
		return st, nil
	}

	return nil, err
}

func dropStore(tx engine.Transaction, t Type, opts Options) error {
	idxName := buildIndexName(opts.IndexName, t)
	_, err := tx.Store(idxName)
	if err != nil && err != engine.ErrStoreNotFound {
		return err
	}

	if err == engine.ErrStoreNotFound {
		return nil
	}

	return tx.DropStore(idxName)
}
