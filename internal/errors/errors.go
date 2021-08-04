// +build !debug

package errors

import (
	baseErrors "errors"
	"fmt"

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

func Is(err, original error) bool {
	return err == original
}
