package index

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/value"
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
// Signed and unsigned integers, and floats are stored in Float indexes.
// Booleans are stores in Bool indexes.
type Type byte

// index value types
const (
	Bytes Type = iota + 1
	Float
	Bool
)

// NewTypeFromValueType returns the right index type associated with t.
func NewTypeFromValueType(t value.Type) Type {
	if value.IsNumber(t) {
		return Float
	}

	if t == value.String || t == value.Bytes {
		return Bytes
	}

	if t == value.Bool {
		return Bool
	}

	return 0
}

var (
	// ErrDuplicate is returned when a value is already associated with a key
	ErrDuplicate = errors.New("duplicate")
)

// An Index associates encoded values with keys.
// It is sorted by value following the lexicographic order.
type Index interface {
	// Set associates a value with a key.
	Set(val value.Value, key []byte) error

	// Delete all the references to the key from the index.
	Delete(val value.Value, key []byte) error

	// AscendGreaterOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in increasing order and calls the given function for each pair.
	// If the given function returns an error, the iteration stops and returns that error.
	// If the pivot is nil, starts from the beginning.
	AscendGreaterOrEqual(pivot value.Value, fn func(val value.Value, key []byte) error) error

	// DescendLessOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
	// If the given function returns an error, the iteration stops and returns that error.
	// If the pivot is nil, starts from the end.
	DescendLessOrEqual(pivot value.Value, fn func(val value.Value, key []byte) error) error
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
func EmptyPivot(t value.Type) value.Value {
	return value.Value{Type: t}
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
func (i *listIndex) Set(val value.Value, key []byte) error {
	if len(val.Data) == 0 {
		return errors.New("value cannot be nil")
	}

	st, err := getOrCreateStore(i.tx, val.Type, i.opts)
	if err != nil {
		return err
	}

	v, err := encodeFieldToIndexValue(val)
	if err != nil {
		return err
	}

	buf := make([]byte, 0, len(v)+len(key)+1)
	buf = append(buf, v...)
	buf = append(buf, Separator)
	buf = append(buf, key...)

	return st.Put(buf, nil)
}

func (i *listIndex) Delete(val value.Value, key []byte) error {
	v, err := encodeFieldToIndexValue(val)
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

func (i *listIndex) AscendGreaterOrEqual(pivot value.Value, fn func(val value.Value, key []byte) error) error {
	st, err := getStore(i.tx, pivot.Type, i.opts)
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
		f, err := decodeIndexValueToField(pivot.Type, k[:idx])
		if err != nil {
			return err
		}

		return fn(f, k[idx+1:])
	})
}

func (i *listIndex) DescendLessOrEqual(pivot value.Value, fn func(val value.Value, key []byte) error) error {
	st, err := getStore(i.tx, pivot.Type, i.opts)
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
		f, err := decodeIndexValueToField(pivot.Type, k[:idx])
		if err != nil {
			return err
		}

		return fn(f, k[idx+1:])
	})
}

// uniqueIndex is an implementation that associates a value with a exactly one key.
type uniqueIndex struct {
	tx   engine.Transaction
	opts Options
}

// Set associates a value with exactly one key.
// If the association already exists, it returns an error.
func (i *uniqueIndex) Set(val value.Value, key []byte) error {
	if len(val.Data) == 0 {
		return errors.New("value cannot be nil")
	}

	v, err := encodeFieldToIndexValue(val)
	if err != nil {
		return err
	}

	st, err := getOrCreateStore(i.tx, val.Type, i.opts)
	if err != nil {
		return err
	}

	_, err = st.Get(v)
	if err == nil {
		return ErrDuplicate
	}
	if err != engine.ErrKeyNotFound {
		return err
	}

	return st.Put(v, key)
}

func (i *uniqueIndex) Delete(val value.Value, key []byte) error {
	v, err := encodeFieldToIndexValue(val)
	if err != nil {
		return err
	}

	st, err := getOrCreateStore(i.tx, val.Type, i.opts)
	if err != nil {
		return err
	}

	return st.Delete(v)
}

func (i *uniqueIndex) AscendGreaterOrEqual(pivot value.Value, fn func(val value.Value, key []byte) error) error {
	st, err := getStore(i.tx, pivot.Type, i.opts)
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

	return st.AscendGreaterOrEqual(v, func(vv []byte, key []byte) error {
		f, err := decodeIndexValueToField(pivot.Type, vv)
		if err != nil {
			return err
		}

		return fn(f, key)
	})
}

func (i *uniqueIndex) DescendLessOrEqual(pivot value.Value, fn func(val value.Value, key []byte) error) error {
	st, err := getStore(i.tx, pivot.Type, i.opts)
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

	return st.DescendLessOrEqual(v, func(vv []byte, key []byte) error {
		f, err := decodeIndexValueToField(pivot.Type, vv)
		if err != nil {
			return err
		}

		return fn(f, key)
	})
}

func encodeFieldToIndexValue(val value.Value) ([]byte, error) {
	if len(val.Data) > 0 && value.IsNumber(val.Type) && val.Type != value.Float64 {
		x, err := val.DecodeToFloat64()
		if err != nil {
			return nil, err
		}

		val = value.NewFloat64(x)
	}

	return val.Data, nil
}

func decodeIndexValueToField(vt value.Type, data []byte) (value.Value, error) {
	t := NewTypeFromValueType(vt)
	switch t {
	case Bytes:
		return value.Value{Type: value.Bytes, Data: data}, nil
	case Float:
		return value.Value{Type: value.Float64, Data: data}, nil
	case Bool:
		return value.Value{Type: value.Bool, Data: data}, nil
	}

	return value.Value{}, fmt.Errorf("unknown index type %d", vt)
}

func getOrCreateStore(tx engine.Transaction, t value.Type, opts Options) (engine.Store, error) {
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

func getStore(tx engine.Transaction, t value.Type, opts Options) (engine.Store, error) {
	idxName := buildIndexName(opts.IndexName, NewTypeFromValueType(t))
	st, err := tx.Store(idxName)
	if err == nil || err == engine.ErrStoreNotFound {
		return st, nil
	}

	return nil, err
}
