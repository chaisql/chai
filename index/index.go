package index

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
	"github.com/genjidb/genji/document/encoding/msgpack"
	"github.com/genjidb/genji/engine"
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
	Array
	Document
)

// NewTypeFromValueType returns the right index type associated with t.
func NewTypeFromValueType(t document.ValueType) Type {
	switch {
	case t.IsNumber():
		return Float
	case t == document.TextValue || t == document.BlobValue:
		return Bytes
	case t == document.BoolValue:
		return Bool
	case t == document.ArrayValue:
		return Array
	case t == document.DocumentValue:
		return Document
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

func buildIndexName(name []byte, t Type) []byte {
	var buf bytes.Buffer

	// We can deterministically set the size of the buffer.
	// The last 2 bytes are for the separator and the Type t.
	buf.Grow(len(StorePrefix) + len(name) + 2)

	buf.WriteString(StorePrefix)
	buf.Write(name)
	buf.WriteByte(separator)
	buf.WriteByte(byte(t))

	return buf.Bytes()
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

// EncodeFieldToIndexValue returns a byte array that represents the value in such
// a way that can be compared for ordering and indexing
func EncodeFieldToIndexValue(val document.Value) ([]byte, error) {
	if val.V != nil && val.Type.IsNumber() && val.Type != document.DoubleValue {
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
		return document.NewDoubleValue(f), err
	case Bool:
		b, err := encoding.DecodeBool(data)
		return document.NewBoolValue(b), err
	case Array:
		return document.NewArrayValue(msgpack.DecodeArray(data)), nil
	case Document:
		return document.NewDocumentValue(msgpack.DecodeDocument(data)), nil
	}

	return document.Value{}, fmt.Errorf("unknown index type %d", t)
}

func getOrCreateStore(tx engine.Transaction, t document.ValueType, name string) (engine.Store, error) {
	idxName := buildIndexName([]byte(name), NewTypeFromValueType(t))
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
	idxName := buildIndexName([]byte(name), t)
	st, err := tx.GetStore(idxName)
	if err == nil || err == engine.ErrStoreNotFound {
		return st, nil
	}

	return nil, err
}

func dropStore(tx engine.Transaction, t Type, name string) error {
	idxName := buildIndexName([]byte(name), t)
	_, err := tx.GetStore(idxName)
	if err != nil && err != engine.ErrStoreNotFound {
		return err
	}

	if err == engine.ErrStoreNotFound {
		return nil
	}

	return tx.DropStore(idxName)
}
