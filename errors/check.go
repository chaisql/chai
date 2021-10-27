//go:build !debug
// +build !debug

package errors

func IsAlreadyExistsError(err error) bool {
	switch err.(type) {
	case AlreadyExistsError, *AlreadyExistsError:
		return true
	default:
		return false
	}
}

func IsNotFoundError(err error) bool {
	switch err.(type) {
	case NotFoundError, *NotFoundError:
		return true
	default:
		return false
	}
}
