package errors

import (
	"fmt"

	"github.com/cockroachdb/errors"
)

// AlreadyExistsError is returned when to create a table, an index or a sequence
// with a name that is already used by another resource.
type AlreadyExistsError struct {
	Name string
}

func (a AlreadyExistsError) Error() string {
	return fmt.Sprintf("%q already exists", a.Name)
}

func IsAlreadyExistsError(err error) bool {
	for err != nil {
		switch err.(type) {
		case *AlreadyExistsError, AlreadyExistsError:
			return true
		}
		err = errors.Unwrap(err)
	}

	return false
}

// NotFoundError is returned when the requested table, index or sequence
// doesn't exist.
type NotFoundError struct {
	Name string
}

func NewDocumentNotFoundError() error {
	return NewNotFoundError("document")
}

func NewNotFoundError(name string) error {
	return &NotFoundError{Name: name}
}

func (a NotFoundError) Error() string {
	if a.Name == "document" {
		return "document not found"
	}

	return fmt.Sprintf("%q not found", a.Name)
}

func IsNotFoundError(err error) bool {
	for err != nil {
		switch err.(type) {
		case *NotFoundError, NotFoundError:
			return true
		}
		err = errors.Unwrap(err)
	}

	return false
}
