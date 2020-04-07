// +build wasm tinygo

package bytesutil

// compareBytes is a proxy for bytes.Compare.
// it is meant to be overrided by another function for wasm builds.
// https://github.com/tinygo-org/tinygo/issues/1034
func CompareBytes(a []byte, b []byte) int {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	if i < len(a) && i < len(b) {
		if a[i] < b[i] {
			return -1
		}

		return 1
	}

	if i < len(a) {
		return 1
	}
	if i < len(b) {
		return -1
	}

	return 0
}
