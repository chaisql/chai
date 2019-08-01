package index

import "github.com/asdine/genji/engine"

// An Index associates encoded values with recordIDs.
// It is sorted by value following the lexicographic order.
type Index struct {
	Store engine.Store
}

// Set associates a value with a recordID. It is possible to associate multiple recordIDs for the same value
// but a recordID can be associated to only one value.
func (i *Index) Set(value []byte, recordID []byte) error {
	return nil
}

// Delete the recordID from the index.
func (i *Index) Delete(recordID []byte) error {
	return nil
}

// AscendGreaterOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in increasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is nil, starts from the beginning.
func (i *Index) AscendGreaterOrEqual(pivot []byte, fn func(k, v []byte) error) error { return nil }

// DescendLessOrEqual seeks for the pivot and then goes through all the subsequent key value pairs in descreasing order and calls the given function for each pair.
// If the given function returns an error, the iteration stops and returns that error.
// If the pivot is nil, starts from the end.
func (i *Index) DescendLessOrEqual(pivot []byte, fn func(k, v []byte) error) error { return nil }
