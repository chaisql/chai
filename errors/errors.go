package errors

import (
	"errors"

	"github.com/genjidb/genji/internal/stringutil"
)

var (
	// ErrTableNotFound is returned when the targeted table doesn't exist.
	ErrTableNotFound = errors.New("table not found")

	// ErrIndexNotFound is returned when the targeted index doesn't exist.
	ErrIndexNotFound = errors.New("index not found")

	// ErrDocumentNotFound is returned when no document is associated with the provided key.
	ErrDocumentNotFound = errors.New("document not found")

	// ErrDuplicateDocument is returned when another document is already associated with a given key, primary key,
	// or if there is a unique index violation.
	ErrDuplicateDocument = errors.New("duplicate document")
)

// AlreadyExistsError is returned when to create a table or an index
// with a name that is already used by another resource.
type AlreadyExistsError struct {
	Name string
}

func (a AlreadyExistsError) Error() string {
	return stringutil.Sprintf("%q already exists", a.Name)
}
