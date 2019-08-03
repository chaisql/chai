package index

import (
	"bytes"
	"errors"

	"github.com/asdine/genji/engine"
)

const (
	separator byte = 0x1E
)

// An Index associates encoded values with recordIDs.
// It is sorted by value following the lexicographic order.
type Index struct {
	Store engine.Store
}

// Set associates a value with a recordID. It is possible to associate multiple recordIDs for the same value
// but a recordID can be associated to only one value.
func (i *Index) Set(value []byte, recordID []byte) error {
	if len(value) == 0 {
		return errors.New("value cannot be nil")
	}

	buf := make([]byte, 0, len(value)+len(recordID)+1)
	buf = append(buf, value...)
	buf = append(buf, separator)
	buf = append(buf, recordID...)

	return i.Store.Put(buf, nil)
}

// Delete the recordID from the index.
func (i *Index) Delete(recordID []byte) error {
	suffix := make([]byte, len(recordID)+1)
	suffix[0] = separator
	copy(suffix[1:], recordID)

	errStop := errors.New("stop")

	err := i.Store.AscendGreaterOrEqual(nil, func(k []byte, v []byte) error {
		if bytes.HasSuffix(k, suffix) {
			err := i.Store.Delete(k)
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

// AscendGreaterOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in increasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is nil, starts from the beginning.
func (i *Index) AscendGreaterOrEqual(pivot []byte, fn func(value []byte, recordID []byte) error) error {
	return i.Store.AscendGreaterOrEqual(pivot, func(k, v []byte) error {
		idx := bytes.LastIndexByte(k, separator)
		return fn(k[:idx], k[idx+1:])
	})
}

// DescendLessOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is nil, starts from the end.
func (i *Index) DescendLessOrEqual(pivot []byte, fn func(k, v []byte) error) error {
	if len(pivot) > 0 {
		// ensure the pivot is bigger than the requested value so it doesn't get skipped.
		pivot = append(pivot, separator, 0xFF)
	}
	return i.Store.DescendLessOrEqual(pivot, func(k, v []byte) error {
		idx := bytes.LastIndexByte(k, separator)
		return fn(k[:idx], k[idx+1:])
	})
}
