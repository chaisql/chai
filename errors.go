package genji

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

	// ErrRecordNotFound is returned when no record is associated with the provided key.
	ErrRecordNotFound = errors.New("record not found")

	// ErrDuplicateRecord is returned when another record is already associated with a given key, primary key,
	// or if there is a unique index violation.
	ErrDuplicateRecord = errors.New("duplicate record")
)
