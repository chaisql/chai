// +build debug

package errors

import (
	baseErrors "errors"
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
	length := runtime.Callers(3, stack[:]) // 3, because we also want to skip _new
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
		err = stringutil.Errorf("%v", e)
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
	return wrap(stringutil.Errorf(format, a...), 1)
}
