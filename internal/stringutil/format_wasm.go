package stringutil

import (
	"io"
)

// Sprintf calls a custom version of sprintf for wasm builds.
func Sprintf(format string, a ...interface{}) string {
	return sprintf(format, a...)
}

// Fprintf calls a custom version of fprintf for wasm builds.
func Fprintf(w io.Writer, format string, a ...interface{}) (n int, err error) {
	return fprintf(w, format, a...)
}

// Errorf calls a custom version of ErrorF for wasm builds.
func Errorf(format string, a ...interface{}) error {
	return errorf(format, a...)
}
