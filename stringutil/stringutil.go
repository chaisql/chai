// +build !wasm

package stringutil

import "fmt"

// Errorf calls stringutil.Errorf.
// In wasm build it is replaced by a custom version.
func Errorf(format string, a ...interface{}) error {
	return fmt.Errorf(format, a...)
}
