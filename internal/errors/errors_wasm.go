package errors

import (
	baseErrors "errors"

	"github.com/genjidb/genji/internal/stringutil"
)

func New(e interface{}) error {
	if e == nil {
		return nil
	}
	return baseErrors.New(e)
}

func Errorf(format string, args ...interface{}) error {
	return stringutil.Errorf(format, args...)
}

// Is behaves exactly like the standard library errors.Is function, but skips
// an internal optimization because tinygo does not support it yet since it
// depends on reflect (reflectlite to be precise, but that one is currently just
// calling reflect).
func Is(err, target error) bool {
	if target == nil {
		return err == target
	}

	for {
		if x, ok := err.(interface{ Is(error) bool }); ok && x.Is(target) {
			return true
		}
		if err = Unwrap(err); err == nil {
			return false
		}
	}
}

func As(err error, target interface{}) bool {
	panic("not implemented")
}
