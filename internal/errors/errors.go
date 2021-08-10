// +build !debug

// Package errors provides a simple API to create and compare errors. A debug version of this package
// exists, but captures stacktraces when error are created or wrapped. It is accessible through the
// the "debug" build tag.
package errors

import (
	baseErrors "errors"

	"github.com/genjidb/genji/internal/stringutil"
)

// New takes either a string, an existing error and returns a standard error. If the error is nil,
// it returns nil, enabling to wrap functions that returns an error directly.
// If a string is passed, a new error is created based on that string.
// If any other type is passed, it will panic.
func New(e interface{}) error {
	if e == nil {
		// This enables to not have to write conditional and just wrap the return expression
		// when doing things like:
		// f := func() err {}
		// return f() can now be written return errors.New(f())
		return nil
	}
	switch e := e.(type) {
	case error:
		return e
	case string:
		return baseErrors.New(e)
	default:
		panic(stringutil.Sprintf("invalid value to create an error: %#v", e))
	}
}

// Errorf creates an error out of a string. If %w is used to format an error, it will
// only wrap it by concatenation, the wrapped error won't be accessible directly and
// thus cannot be accessed through the Is or As functions from the standard error package.
func Errorf(format string, a ...interface{}) error {
	return stringutil.Errorf(format, a...)
}

// Is performs a simple value comparison between err and original (==).
func Is(err, original error) bool {
	return err == original
}

// Unwrap does nothing and just returns err.
// This function only acts differently when the debug version of this function is used.
func Unwrap(err error) error {
	return err
}
