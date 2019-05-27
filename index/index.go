package index

// An Index associates encoded values with rowids.
// It is sorted by value following the lexicographic order.
type Index interface {
	// Cursor creates an iterator that can traverse the index.
	Cursor() Cursor
	// Set associates a value with a rowid. It is possible to associate multiple rowids for the same value
	// but a rowid can be associated to only one value.
	Set(value []byte, rowid []byte) error
	// Delete the rowid from the index.
	Delete(rowid []byte) error
}

// A Cursor can traverse an index in any direction.
type Cursor interface {
	// First moves the cursor to the first item of the index.
	// If the index is empty, it returns (nil, nil)
	First() (value []byte, rowid []byte)
	// First moves the cursor to the last item of the index.
	// If the index is empty, it returns (nil, nil)
	Last() (value []byte, rowid []byte)
	// Next moves the cursor to the next value-rowid pair.
	// If there is no more items left, or if the index is empty, it returns (nil, nil)
	Next() (value []byte, rowid []byte)
	// Prev moves the cursor to the previous value-rowid pair.
	// If there is no more items left, or if the index is empty, it returns (nil, nil)
	Prev() (value []byte, rowid []byte)
	// Seek for a value in the index by moving the cursor to it.
	// If the value doesn't exists, the cursor is positioned to the next value.
	// If there is no more items left, or if the index is empty, it returns (nil, nil)
	Seek(seek []byte) (value []byte, rowid []byte)
}
