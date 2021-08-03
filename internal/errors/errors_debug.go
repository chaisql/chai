// +build debug

package errors

import (
	baseErrors "errors"
	"fmt"
	"runtime"

	"github.com/genjidb/genji/internal/stringutil"
)

func New(e interface{}) error {
	if e == nil {
		// This enables to not have to write conditional and just wrap the return expression
		// when doing things like:
		// f := func() err {}
		// return f() can now be written return errors.New(f())
		return nil
	}
	return _new(e)
}

func Errorf(format string, a ...interface{}) error {
	return errorf(format, a...)
}

func As(err error, target interface{}) bool {
	return as(err, target)
}

func Is(err, original error) bool {
	return is(err, original)
}

// The maximum number of stackframes on any error.
var MaxStackDepth = 50

func _new(e interface{}) *Error {
	var err error
	switch e := e.(type) {
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
// fmt.Errorf("%v"). The skip parameter indicates how far up the stack
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
		err = fmt.Errorf("%v", e)
	}
	stack := make([]uintptr, MaxStackDepth)
	length := runtime.Callers(2+skip, stack[:])
	return &Error{
		Err:   err,
		stack: stack[:length],
	}
}

// Errorf creates a new error with the given message. You can use it
// as a drop-in replacement for fmt.Errorf() to provide descriptive
// errors in return values.
func errorf(format string, a ...interface{}) *Error {
	return wrap(fmt.Errorf(format, a...), 1)
}

// find error in any wrapped error
func as(err error, target interface{}) bool {
	return baseErrors.As(err, target)
}

// Is detects whether the error is equal to a given error. Errors
// are considered equal by this function if they are matched by errors.Is
// or if their contained errors are matched through errors.Is
func is(e error, original error) bool {
	if baseErrors.Is(e, original) {
		return true
	}
	if e, ok := e.(*Error); ok {
		return is(e.Err, original)
	}
	if original, ok := original.(*Error); ok {
		return is(e, original.Err)
	}
	return false
}
