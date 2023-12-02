package dbutil

import "os"

// CanReadFromStandardInput returns whether there is data to be read
// in stdin.
func CanReadFromStandardInput() bool {
	fi, _ := os.Stdin.Stat()
	m := fi.Mode()
	return (m & os.ModeNamedPipe) != 0
}
