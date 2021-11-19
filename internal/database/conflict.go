package database

import "github.com/genjidb/genji/types"

// OnInsertConflictAction is a function triggered when trying to insert a document that already exists.
// This function is triggered if the key is duplicated or if there is a unique constraint violation on one
// of the fields of the document.
type OnInsertConflictAction func(t *Table, key []byte, d types.Document, err error) (types.Document, error)

// OnInsertConflictDoNothing ignores the duplicate error and returns nothing.
func OnInsertConflictDoNothing(t *Table, key []byte, d types.Document, err error) (types.Document, error) {
	return nil, nil
}

// OnInsertConflictDoReplace replaces the conflicting document with d.
func OnInsertConflictDoReplace(t *Table, key []byte, d types.Document, err error) (types.Document, error) {
	if key == nil {
		return d, err
	}

	return t.Replace(key, d)
}
