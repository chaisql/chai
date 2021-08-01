// +build !debug

package errors

import (
	baseErrors "errors"
	"fmt"

	"github.com/genjidb/genji/internal/stringutil"
)

func New(e interface{}) error {
	switch e := e.(type) {
	case error:
		return e
	case string:
		return baseErrors.New(e)
	default:
		panic(stringutil.Sprintf("invalid value to create an error: %#v", e))
	}
}

func Errorf(format string, a ...interface{}) error {
	return fmt.Errorf(format, a...)
}

func As(err error, target interface{}) bool {
	return baseErrors.As(err, target)
}

func Is(err, original error) bool {
	return baseErrors.Is(err, original)
}
