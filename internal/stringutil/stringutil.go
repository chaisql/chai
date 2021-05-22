// +build !wasm

package stringutil

import (
	"fmt"
	"io"
)

// Sprintf calls fmt.Sprintf.
// During wasm builds it is replaced by a custom version.
func Sprintf(format string, a ...interface{}) string {
	return fmt.Sprintf(format, a...)
}

// Fprintf calls fmt.Fprintf.
// During wasm builds it is replaced by a custom version.
func Fprintf(w io.Writer, format string, a ...interface{}) (n int, err error) {
	return fmt.Fprintf(w, format, a...)
}

// Errorf calls fmt.Errorf.
// During wasm builds it is replaced by a custom version.
func Errorf(format string, a ...interface{}) error {
	return fmt.Errorf(format, a...)
}
