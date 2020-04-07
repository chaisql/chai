// +build tinygo wasm

package scanner

import (
	"bufio"
	"io"
	"strings"
	"sync"
)

var keywordsInitializer sync.Once

func initKeywords() {
	keywords = make(map[string]Token)
	for tok := keywordBeg + 1; tok < keywordEnd; tok++ {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
	for _, tok := range []Token{AND, OR, TRUE, FALSE, NULL} {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
}

// NewScanner returns a new instance of Scanner.
func NewScanner(r io.Reader) *Scanner {
	keywordsInitializer.Do(initKeywords)
	return &Scanner{r: &reader{r: bufio.NewReaderSize(r, 128)}}
}
