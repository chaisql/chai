// +build !wasm
// +build !tinygo

package bytesutil

import "bytes"

// CompareBytes is a proxy for bytes.Compare.
// it is meant to be overrided by another function for wasm builds.
// https://github.com/tinygo-org/tinygo/issues/1034
func CompareBytes(a []byte, b []byte) int {
	return bytes.Compare(a, b)
}
