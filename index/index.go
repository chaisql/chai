package index

import (
	"errors"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
)

const (
	// storePrefix is the prefix used to name the index stores.
	storePrefix = "i"
)

var valueTypes = []document.ValueType{
	document.NullValue,
	document.BoolValue,
	document.DoubleValue,
	document.DurationValue,
	document.TextValue,
	document.BlobValue,
	document.ArrayValue,
	document.DocumentValue,
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
	// If val is equal to the pivot, isEqual is set to true.
	AscendGreaterOrEqual(pivot document.Value, fn func(val []byte, key []byte, isEqual bool) error) error

	// DescendLessOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
	// If the given function returns an error, the iteration stops and returns that error.
	// If the pivot is nil, starts from the end.
	// If val is equal to the pivot, isEqual is set to true.
	DescendLessOrEqual(pivot document.Value, fn func(val []byte, key []byte, isEqual bool) error) error

	// Truncate deletes all the index data.
	Truncate() error
}

// NewListIndex creates an index that associates a value with a list of keys.
func NewListIndex(tx engine.Transaction, idxName string) *ListIndex {
	return &ListIndex{
		tx:        tx,
		name:      idxName,
		storeName: append([]byte(storePrefix), idxName...),
	}
}

// NewUniqueIndex creates an index that associates a value with a exactly one key.
func NewUniqueIndex(tx engine.Transaction, idxName string) *UniqueIndex {
	return &UniqueIndex{
		tx:        tx,
		name:      idxName,
		storeName: append([]byte(storePrefix), idxName...),
	}
}

var errStop = errors.New("stop")

func getOrCreateStore(tx engine.Transaction, name []byte) (engine.Store, error) {
	st, err := tx.GetStore(name)
	if err == nil {
		return st, nil
	}

	if err != engine.ErrStoreNotFound {
		return nil, err
	}

	err = tx.CreateStore(name)
	if err != nil {
		return nil, err
	}

	return tx.GetStore(name)
}
