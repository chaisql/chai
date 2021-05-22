package errors

import "errors"

var (
	// ErrTableNotFound is returned when the targeted table doesn't exist.
	ErrTableNotFound = errors.New("table not found")

	// ErrTableAlreadyExists is returned when attempting to create a table with the
	// same name as an existing one.
	ErrTableAlreadyExists = errors.New("table already exists")

	// ErrIndexNotFound is returned when the targeted index doesn't exist.
	ErrIndexNotFound = errors.New("index not found")

	// ErrIndexAlreadyExists is returned when attempting to create an index with the
	// same name as an existing one.
	ErrIndexAlreadyExists = errors.New("index already exists")

	// ErrDocumentNotFound is returned when no document is associated with the provided key.
	ErrDocumentNotFound = errors.New("document not found")

	// ErrDuplicateDocument is returned when another document is already associated with a given key, primary key,
	// or if there is a unique index violation.
	ErrDuplicateDocument = errors.New("duplicate document")
)
