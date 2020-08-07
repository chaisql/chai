package index

import (
	"bytes"
	"errors"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/key"
)

const (
	// StorePrefix is the prefix used to name the index store.
	StorePrefix = "i" + string(separator)

	separator byte = 0x1E
)

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
	AscendGreaterOrEqual(pivot *Pivot, fn func(val []byte, key []byte) error) error

	// DescendLessOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
	// If the given function returns an error, the iteration stops and returns that error.
	// If the pivot is nil, starts from the end.
	DescendLessOrEqual(pivot *Pivot, fn func(val []byte, key []byte) error) error

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

func buildIndexName(name []byte, t document.ValueType) []byte {
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
	EncodedValue []byte
	Type         document.ValueType
}

// NewPivot encodes v and returns a pivot for this type.
func NewPivot(v document.Value) (*Pivot, error) {
	enc, err := key.EncodeValue(v)
	if err != nil {
		return nil, err
	}

	return &Pivot{
		EncodedValue: enc,
		Type:         v.Type,
	}, nil
}

func getOrCreateStore(tx engine.Transaction, t document.ValueType, name string) (engine.Store, error) {
	idxName := buildIndexName([]byte(name), t)
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

func getStore(tx engine.Transaction, t document.ValueType, name string) (engine.Store, error) {
	idxName := buildIndexName([]byte(name), t)
	st, err := tx.GetStore(idxName)
	if err == nil || err == engine.ErrStoreNotFound {
		return st, nil
	}

	return nil, err
}

func dropStore(tx engine.Transaction, t document.ValueType, name string) error {
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
