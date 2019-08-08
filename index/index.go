package index

import (
	"bytes"
	"errors"

	"github.com/asdine/genji/engine"
)

const (
	separator byte = 0x1E
)

var (
	// ErrDuplicate is returned when a value is already associated with a recordID
	ErrDuplicate = errors.New("duplicate")
)

// An Index associates encoded values with recordIDs.
// It is sorted by value following the lexicographic order.
type Index interface {
	// Set associates a value with a recordID.
	Set(value []byte, recordID []byte) error

	// Delete all the references to the recordID from the index.
	Delete(recordID []byte) error

	// AscendGreaterOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in increasing order and calls the given function for each pair.
	// If the given function returns an error, the iteration stops and returns that error.
	// If the pivot is nil, starts from the beginning.
	AscendGreaterOrEqual(pivot []byte, fn func(value []byte, recordID []byte) error) error

	// DescendLessOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
	// If the given function returns an error, the iteration stops and returns that error.
	// If the pivot is nil, starts from the end.
	DescendLessOrEqual(pivot []byte, fn func(k, v []byte) error) error
}

// Options of the index.
type Options struct {
	// If set to true, values will be associated with at most one recordID. False by default.
	Unique bool
}

// New creates an index with the given store and options.
func New(store engine.Store, opts Options) Index {
	if opts.Unique {
		return &uniqueIndex{
			store: store,
		}
	}

	return &listIndex{
		store: store,
	}
}

// listIndex is an implementation that associates a value with a list of recordIDs.
type listIndex struct {
	store engine.Store
}

// Set associates a value with a recordID. It is possible to associate multiple recordIDs for the same value
// but a recordID can be associated to only one value.
func (i *listIndex) Set(value []byte, recordID []byte) error {
	if len(value) == 0 {
		return errors.New("value cannot be nil")
	}

	buf := make([]byte, 0, len(value)+len(recordID)+1)
	buf = append(buf, value...)
	buf = append(buf, separator)
	buf = append(buf, recordID...)

	return i.store.Put(buf, nil)
}

func (i *listIndex) Delete(recordID []byte) error {
	suffix := make([]byte, len(recordID)+1)
	suffix[0] = separator
	copy(suffix[1:], recordID)

	errStop := errors.New("stop")

	err := i.store.AscendGreaterOrEqual(nil, func(k []byte, v []byte) error {
		if bytes.HasSuffix(k, suffix) {
			err := i.store.Delete(k)
			if err != nil {
				return err
			}
			return errStop
		}

		return nil
	})

	if err != errStop {
		return err
	}

	return nil
}

func (i *listIndex) AscendGreaterOrEqual(pivot []byte, fn func(value []byte, recordID []byte) error) error {
	return i.store.AscendGreaterOrEqual(pivot, func(k, v []byte) error {
		idx := bytes.LastIndexByte(k, separator)
		return fn(k[:idx], k[idx+1:])
	})
}

func (i *listIndex) DescendLessOrEqual(pivot []byte, fn func(k, v []byte) error) error {
	if len(pivot) > 0 {
		// ensure the pivot is bigger than the requested value so it doesn't get skipped.
		pivot = append(pivot, separator, 0xFF)
	}
	return i.store.DescendLessOrEqual(pivot, func(k, v []byte) error {
		idx := bytes.LastIndexByte(k, separator)
		return fn(k[:idx], k[idx+1:])
	})
}

// uniqueIndex is an implementation that associates a value with a exactly one recordID.
type uniqueIndex struct {
	store engine.Store
}

// Set associates a value with exactly one recordID.
// If the association already exists, it returns an error.
func (i *uniqueIndex) Set(value []byte, recordID []byte) error {
	if len(value) == 0 {
		return errors.New("value cannot be nil")
	}

	_, err := i.store.Get(value)
	if err == nil {
		return ErrDuplicate
	}
	if err != engine.ErrKeyNotFound {
		return err
	}

	return i.store.Put(value, recordID)
}

func (i *uniqueIndex) Delete(recordID []byte) error {
	var toDelete [][]byte

	err := i.store.AscendGreaterOrEqual(nil, func(value []byte, rID []byte) error {
		if bytes.Equal(recordID, rID) {
			toDelete = append(toDelete, value)
		}

		return nil
	})

	if err != nil {
		return err
	}

	for _, v := range toDelete {
		err := i.store.Delete(v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (i *uniqueIndex) AscendGreaterOrEqual(pivot []byte, fn func(value []byte, recordID []byte) error) error {
	return i.store.AscendGreaterOrEqual(pivot, fn)
}

func (i *uniqueIndex) DescendLessOrEqual(pivot []byte, fn func(k, v []byte) error) error {
	return i.store.DescendLessOrEqual(pivot, fn)
}
