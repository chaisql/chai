package parser_test

import (
	"strings"
	"testing"

	"github.com/asdine/genji/query/sql/parser"
)

// Ensure the scanner can scan tokens correctly.
func TestScanner_Scan(t *testing.T) {
	var tests = []struct {
		s   string
		tok parser.Token
		lit string
	}{
		// Special tokens (EOF, ILLEGAL, WS)
		{s: ``, tok: parser.EOF},
		{s: `#`, tok: parser.ILLEGAL, lit: `#`},
		{s: ` `, tok: parser.WS, lit: " "},
		{s: "\t", tok: parser.WS, lit: "\t"},
		{s: "\n", tok: parser.WS, lit: "\n"},

		// Misc characters
		{s: `*`, tok: parser.ASTERISK, lit: "*"},

		// Identifiers
		{s: `foo`, tok: parser.IDENT, lit: `foo`},
		{s: `Zx12_3U_-`, tok: parser.IDENT, lit: `Zx12_3U_`},

		// Keywords
		{s: `FROM`, tok: parser.FROM, lit: "FROM"},
		{s: `SELECT`, tok: parser.SELECT, lit: "SELECT"},
		{s: `WHERE`, tok: parser.WHERE, lit: "WHERE"},
	}

	for i, tt := range tests {
		s := parser.NewScanner(strings.NewReader(tt.s))
		tok, lit := s.Scan()
		if tt.tok != tok {
			t.Errorf("%d. %q token mismatch: exp=%q got=%q <%q>", i, tt.s, tt.tok, tok, lit)
		} else if tt.lit != lit {
			t.Errorf("%d. %q literal mismatch: exp=%q got=%q", i, tt.s, tt.lit, lit)
		}
	}
}
