// +build debug

// Package errors provides a simple API to create and compare errors.
// It captures the stacktrace when an error is created or wrapped, which can be then be inspected for debugging purposes.
// This package, compiled with the "debug" build tag is only meant to ease development and should not be used otherwise.
package errors

import (
	baseErrors "errors"
	"runtime"

	"github.com/genjidb/genji/internal/stringutil"
)

// New takes either a string, an existing error and returns a wrapped error that allows to inspect the stacktrace at the moment
// of its creation. If the error is nil, it returns nil, enabling to wrap functions that returns an error directly.
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
	err := _new(e)
	if len(err.stack) > 1 {
		// Truncate the call to _new
		err.stack = err.stack[1:]
	}
	return err
}

// Errorf creates an error that includes the stracktrace, out of a string. If %w is used to format an error, it will
// only wrap it by concatenation, the wrapped error won't be accessible directly and
// thus cannot be accessed through the Is or As functions from the standard error package.
func Errorf(format string, a ...interface{}) error {
	return errorf(format, a...)
}

// Is performs a value comparison between err and the target, unwrapping them if necessary.
func Is(err, target error) bool {
	if err == target {
		return true
	}
	if e, ok := err.(*Error); ok {
		if t, ok := target.(*Error); ok {
			return e.Err == t.Err
		} else {
			return e.Err == target
		}
	}
	if target, ok := target.(*Error); ok {
		return err == target.Err
	}
	return false
}

// Unwrap returns the underlying error, or the error itself if err is not an *errors.Error.
func Unwrap(err error) error {
	if err == nil {
		return nil
	}
	if e, ok := err.(*Error); ok {
		return e.Err
	}
	return err
}

// The maximum number of stackframes on any error.
var MaxStackDepth = 32

func _new(e interface{}) *Error {
	var err error
	switch e := e.(type) {
	case *Error:
		err = e.Err
	case error:
		err = e
	case string:
		err = baseErrors.New(e)
	default:
		panic(stringutil.Sprintf("invalid value to create an error: %#v", e))
	}
	stack := make([]uintptr, MaxStackDepth)
	length := runtime.Callers(2, stack[:])
	return &Error{
		Err:   err,
		stack: stack[:length],
	}
}

// wrap makes an Error from the given value. If that value is already an
// error then it will be used directly, if not, it will be passed to
// stringutil.Errorf("%v"). The skip parameter indicates how far up the stack
// to start the stacktrace. 0 is from the current call, 1 from its caller, etc.
func wrap(e interface{}, skip int) *Error {
	if e == nil {
		return nil
	}
	var err error
	switch e := e.(type) {
	case *Error:
		return e
	case error:
		err = e
	default:
		err = stringutil.Errorf("%v", e)
	}
	stack := make([]uintptr, MaxStackDepth)
	length := runtime.Callers(2+skip, stack[:])
	return &Error{
		Err:   err,
		stack: stack[:length],
	}
}

func errorf(format string, a ...interface{}) *Error {
	return wrap(stringutil.Errorf(format, a...), 1)
}
