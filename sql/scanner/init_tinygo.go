// +build tinygo wasm

package scanner

import (
	"bufio"
	"io"
	"sync"
)

var keywordsInitializer sync.Once

// NewScanner returns a new instance of Scanner.
func NewScanner(r io.Reader) *Scanner {
	keywordsInitializer.Do(initKeywords)
	return &Scanner{r: &reader{r: bufio.NewReaderSize(r, 128)}}
}
