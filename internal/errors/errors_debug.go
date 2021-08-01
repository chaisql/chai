// +build debug

package errors

func New(e interface{}) error {
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
