//go:build !debug
// +build !debug

// Package errors provides a simple API to create and compare errors. A debug version of this package
// exists, but captures stacktraces when error are created or wrapped. It is accessible through the
// the "debug" build tag.
package errors

import (
	baseErrors "errors"

	"github.com/genjidb/genji/internal/stringutil"
)

// New takes a string and returns a standard error.
func New(s string) error {
	return baseErrors.New(s)
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

// Wrap acts as the identity function, unless compiled with the debug tag.
// See the debug version of this package for more.
func Wrap(err error) error {
	return err
}
