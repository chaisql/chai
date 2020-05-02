// +build !wasm
// +build !tinygo

package scanner

import (
	"bufio"
	"io"
)

func init() {
	initKeywords()
}

// NewScanner returns a new instance of Scanner.
func NewScanner(r io.Reader) *Scanner {
	return &Scanner{r: &reader{r: bufio.NewReaderSize(r, 128)}}
}
