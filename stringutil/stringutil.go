// +build !wasm

package stringutil

import "fmt"

// Sprintf calls fmt.Sprintf.
// During wasm builds it is replaced by a custom version.
func Sprintf(format string, a ...interface{}) string {
	return fmt.Sprintf(format, a...)
}

// Errorf calls fmt.Errorf.
// During wasm builds it is replaced by a custom version.
func Errorf(format string, a ...interface{}) error {
	return fmt.Errorf(format, a...)
}
