//go:build debug
// +build debug

package errors

import "github.com/genjidb/genji/internal/errors"

func IsAlreadyExistsError(err error) bool {
	switch e := err.(type) {
	case *errors.Error:
		return IsAlreadyExistsError(e.Err)
	case AlreadyExistsError, *AlreadyExistsError:
		return true
	default:
		return false
	}
}

func IsNotFoundError(err error) bool {
	switch e := err.(type) {
	case *errors.Error:
		return IsNotFoundError(e.Err)
	case NotFoundError, *NotFoundError:
		return true
	default:
		return false
	}
}
