package stringutil

// Sprintf calls a custom version of sprintf for wasm builds.
func Sprintf(format string, a ...interface{}) string {
	return sprintf(format, a...)
}

// Errorf calls a custom version of ErrorF for wasm builds.
func Errorf(format string, a ...interface{}) error {
	return errorf(format, a...)
}
