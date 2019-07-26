package index

// An Index associates encoded values with recordIDs.
// It is sorted by value following the lexicographic order.
type Index interface {
	// Cursor creates an iterator that can traverse the index.
	Cursor() Cursor
	// Set associates a value with a recordID. It is possible to associate multiple recordIDs for the same value
	// but a recordID can be associated to only one value.
	Set(value []byte, recordID []byte) error
	// Delete the recordID from the index.
	Delete(recordID []byte) error
}

// A Cursor can traverse an index in any direction.
type Cursor interface {
	// First moves the cursor to the first item of the index.
	// If the index is empty, it returns (nil, nil)
	First() (value []byte, recordID []byte)
	// First moves the cursor to the last item of the index.
	// If the index is empty, it returns (nil, nil)
	Last() (value []byte, recordID []byte)
	// Next moves the cursor to the next value-recordID pair.
	// If there is no more items left, or if the index is empty, it returns (nil, nil)
	Next() (value []byte, recordID []byte)
	// Prev moves the cursor to the previous value-recordID pair.
	// If there is no more items left, or if the index is empty, it returns (nil, nil)
	Prev() (value []byte, recordID []byte)
	// Seek for a value in the index by moving the cursor to it.
	// If the value doesn't exists, the cursor is positioned to the next value.
	// If there is no more items left, or if the index is empty, it returns (nil, nil)
	Seek(seek []byte) (value []byte, recordID []byte)
}
