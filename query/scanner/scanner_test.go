package scanner_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/asdine/genji/query/scanner"
)

// Ensure the scanner can scan tokens correctly.
func TestScanner_Scan(t *testing.T) {
	var tests = []struct {
		s   string
		tok scanner.Token
		lit string
		pos scanner.Pos
	}{
		// Special tokens (EOF, ILLEGAL, WS)
		{s: ``, tok: scanner.EOF},
		{s: `#`, tok: scanner.ILLEGAL, lit: `#`},
		{s: ` `, tok: scanner.WS, lit: " "},
		{s: "\t", tok: scanner.WS, lit: "\t"},
		{s: "\n", tok: scanner.WS, lit: "\n"},
		{s: "\r", tok: scanner.WS, lit: "\n"},
		{s: "\r\n", tok: scanner.WS, lit: "\n"},
		{s: "\rX", tok: scanner.WS, lit: "\n"},
		{s: "\n\r", tok: scanner.WS, lit: "\n\n"},
		{s: " \n\t \r\n\t", tok: scanner.WS, lit: " \n\t \n\t"},
		{s: " foo", tok: scanner.WS, lit: " "},

		// Numeric operators
		{s: `+`, tok: scanner.ADD},
		{s: `-`, tok: scanner.SUB},
		{s: `*`, tok: scanner.MUL},
		{s: `/`, tok: scanner.DIV},
		{s: `%`, tok: scanner.MOD},

		// Logical operators
		{s: `AND`, tok: scanner.AND},
		{s: `and`, tok: scanner.AND},
		{s: `OR`, tok: scanner.OR},
		{s: `or`, tok: scanner.OR},

		{s: `=`, tok: scanner.EQ},
		{s: `<>`, tok: scanner.NEQ},
		{s: `! `, tok: scanner.ILLEGAL, lit: "!"},
		{s: `<`, tok: scanner.LT},
		{s: `<=`, tok: scanner.LTE},
		{s: `>`, tok: scanner.GT},
		{s: `>=`, tok: scanner.GTE},

		// Misc tokens
		{s: `(`, tok: scanner.LPAREN},
		{s: `)`, tok: scanner.RPAREN},
		{s: `,`, tok: scanner.COMMA},
		{s: `;`, tok: scanner.SEMICOLON},
		{s: `.`, tok: scanner.DOT},
		{s: `=~`, tok: scanner.EQREGEX},
		{s: `!~`, tok: scanner.NEQREGEX},
		{s: `:`, tok: scanner.COLON},
		{s: `::`, tok: scanner.DOUBLECOLON},

		// Identifiers
		{s: `foo`, tok: scanner.IDENT, lit: `foo`},
		{s: `_foo`, tok: scanner.IDENT, lit: `_foo`},
		{s: `Zx12_3U_-`, tok: scanner.IDENT, lit: `Zx12_3U_`},
		{s: `"foo"`, tok: scanner.IDENT, lit: `foo`},
		{s: `"foo\\bar"`, tok: scanner.IDENT, lit: `foo\bar`},
		{s: `"foo\bar"`, tok: scanner.BADESCAPE, lit: `\b`, pos: scanner.Pos{Line: 0, Char: 5}},
		{s: `"foo\"bar\""`, tok: scanner.IDENT, lit: `foo"bar"`},
		{s: `test"`, tok: scanner.BADSTRING, lit: "", pos: scanner.Pos{Line: 0, Char: 3}},
		{s: `"test`, tok: scanner.BADSTRING, lit: `test`},
		{s: `$host`, tok: scanner.NAMEDPARAM, lit: `$host`},
		{s: `$"host param"`, tok: scanner.NAMEDPARAM, lit: `$host param`},
		{s: `?`, tok: scanner.POSITIONALPARAM, lit: ""},

		{s: `true`, tok: scanner.TRUE},
		{s: `false`, tok: scanner.FALSE},

		// Strings
		{s: `'testing 123!'`, tok: scanner.STRING, lit: `testing 123!`},
		{s: `'foo\nbar'`, tok: scanner.STRING, lit: "foo\nbar"},
		{s: `'foo\\bar'`, tok: scanner.STRING, lit: "foo\\bar"},
		{s: `'test`, tok: scanner.BADSTRING, lit: `test`},
		{s: "'test\nfoo", tok: scanner.BADSTRING, lit: `test`},
		{s: `'test\g'`, tok: scanner.BADESCAPE, lit: `\g`, pos: scanner.Pos{Line: 0, Char: 6}},

		// Numbers
		{s: `100`, tok: scanner.INTEGER, lit: `100`},
		{s: `100.23`, tok: scanner.NUMBER, lit: `100.23`},
		{s: `.23`, tok: scanner.NUMBER, lit: `.23`},
		//{s: `.`, tok: scanner.ILLEGAL, lit: `.`},
		{s: `10.3s`, tok: scanner.NUMBER, lit: `10.3`},

		// Durations
		{s: `10u`, tok: scanner.DURATIONVAL, lit: `10u`},
		{s: `10µ`, tok: scanner.DURATIONVAL, lit: `10µ`},
		{s: `10ms`, tok: scanner.DURATIONVAL, lit: `10ms`},
		{s: `1s`, tok: scanner.DURATIONVAL, lit: `1s`},
		{s: `10m`, tok: scanner.DURATIONVAL, lit: `10m`},
		{s: `10h`, tok: scanner.DURATIONVAL, lit: `10h`},
		{s: `10d`, tok: scanner.DURATIONVAL, lit: `10d`},
		{s: `10w`, tok: scanner.DURATIONVAL, lit: `10w`},
		{s: `10x`, tok: scanner.DURATIONVAL, lit: `10x`}, // non-duration unit, but scanned as a duration value

		// Keywords
		{s: `ALL`, tok: scanner.ALL},
		{s: `ALTER`, tok: scanner.ALTER},
		{s: `AS`, tok: scanner.AS},
		{s: `ASC`, tok: scanner.ASC},
		{s: `BY`, tok: scanner.BY},
		{s: `DELETE`, tok: scanner.DELETE},
		{s: `DESC`, tok: scanner.DESC},
		{s: `DROP`, tok: scanner.DROP},
		{s: `DURATION`, tok: scanner.DURATION},
		{s: `FROM`, tok: scanner.FROM},
		{s: `INSERT`, tok: scanner.INSERT},
		{s: `INTO`, tok: scanner.INTO},
		{s: `LIMIT`, tok: scanner.LIMIT},
		{s: `OFFSET`, tok: scanner.OFFSET},
		{s: `ORDER`, tok: scanner.ORDER},
		{s: `SELECT`, tok: scanner.SELECT},
		{s: `TO`, tok: scanner.TO},
		{s: `VALUES`, tok: scanner.VALUES},
		{s: `WHERE`, tok: scanner.WHERE},
		{s: `seLECT`, tok: scanner.SELECT}, // case insensitive
	}

	for i, tt := range tests {
		s := scanner.NewScanner(strings.NewReader(tt.s))
		tok, pos, lit := s.Scan()
		if tt.tok != tok {
			t.Errorf("%d. %q token mismatch: exp=%q got=%q <%q>", i, tt.s, tt.tok, tok, lit)
		} else if tt.pos.Line != pos.Line || tt.pos.Char != pos.Char {
			t.Errorf("%d. %q pos mismatch: exp=%#v got=%#v", i, tt.s, tt.pos, pos)
		} else if tt.lit != lit {
			t.Errorf("%d. %q literal mismatch: exp=%q got=%q", i, tt.s, tt.lit, lit)
		}
	}
}

// Ensure the scanner can scan a series of tokens correctly.
func TestScanner_Scan_Multi(t *testing.T) {
	type result struct {
		tok scanner.Token
		pos scanner.Pos
		lit string
	}
	exp := []result{
		{tok: scanner.SELECT, pos: scanner.Pos{Line: 0, Char: 0}, lit: ""},
		{tok: scanner.WS, pos: scanner.Pos{Line: 0, Char: 6}, lit: " "},
		{tok: scanner.IDENT, pos: scanner.Pos{Line: 0, Char: 7}, lit: "value"},
		{tok: scanner.WS, pos: scanner.Pos{Line: 0, Char: 12}, lit: " "},
		{tok: scanner.FROM, pos: scanner.Pos{Line: 0, Char: 13}, lit: ""},
		{tok: scanner.WS, pos: scanner.Pos{Line: 0, Char: 17}, lit: " "},
		{tok: scanner.IDENT, pos: scanner.Pos{Line: 0, Char: 18}, lit: "myseries"},
		{tok: scanner.WS, pos: scanner.Pos{Line: 0, Char: 26}, lit: " "},
		{tok: scanner.WHERE, pos: scanner.Pos{Line: 0, Char: 27}, lit: ""},
		{tok: scanner.WS, pos: scanner.Pos{Line: 0, Char: 32}, lit: " "},
		{tok: scanner.IDENT, pos: scanner.Pos{Line: 0, Char: 33}, lit: "a"},
		{tok: scanner.WS, pos: scanner.Pos{Line: 0, Char: 34}, lit: " "},
		{tok: scanner.EQ, pos: scanner.Pos{Line: 0, Char: 35}, lit: ""},
		{tok: scanner.WS, pos: scanner.Pos{Line: 0, Char: 36}, lit: " "},
		{tok: scanner.STRING, pos: scanner.Pos{Line: 0, Char: 36}, lit: "b"},
		{tok: scanner.EOF, pos: scanner.Pos{Line: 0, Char: 40}, lit: ""},
	}

	// Create a scanner.
	v := `SELECT value from myseries WHERE a = 'b'`
	s := scanner.NewScanner(strings.NewReader(v))

	// Continually scan until we reach the end.
	var act []result
	for {
		tok, pos, lit := s.Scan()
		act = append(act, result{tok, pos, lit})
		if tok == scanner.EOF {
			break
		}
	}

	// Verify the token counts match.
	if len(exp) != len(act) {
		t.Fatalf("token count mismatch: exp=%d, got=%d", len(exp), len(act))
	}

	// Verify each token matches.
	for i := range exp {
		if !reflect.DeepEqual(exp[i], act[i]) {
			t.Fatalf("%d. token mismatch:\n\nexp=%#v\n\ngot=%#v", i, exp[i], act[i])
		}
	}
}

// Ensure the library can correctly scan strings.
func TestScanString(t *testing.T) {
	var tests = []struct {
		in  string
		out string
		err string
	}{
		{in: `""`, out: ``},
		{in: `"foo bar"`, out: `foo bar`},
		{in: `'foo bar'`, out: `foo bar`},
		{in: `"foo\nbar"`, out: "foo\nbar"},
		{in: `"foo\\bar"`, out: `foo\bar`},
		{in: `"foo\"bar"`, out: `foo"bar`},
		{in: `'foo\'bar'`, out: `foo'bar`},

		{in: `"foo` + "\n", out: `foo`, err: "bad string"}, // newline in string
		{in: `"foo`, out: `foo`, err: "bad string"},        // unclosed quotes
		{in: `"foo\xbar"`, out: `\x`, err: "bad escape"},   // invalid escape
	}

	for i, tt := range tests {
		out, err := scanner.ScanString(strings.NewReader(tt.in))
		if tt.err != errstring(err) {
			t.Errorf("%d. %s: error: exp=%s, got=%s", i, tt.in, tt.err, err)
		} else if tt.out != out {
			t.Errorf("%d. %s: out: exp=%s, got=%s", i, tt.in, tt.out, out)
		}
	}
}

// errstring converts an error to its string representation.
func errstring(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

// Test scanning regex
func TestScanRegex(t *testing.T) {
	var tests = []struct {
		in  string
		tok scanner.Token
		lit string
		err string
	}{
		{in: `/^payments\./`, tok: scanner.REGEX, lit: `^payments\.`},
		{in: `/foo\/bar/`, tok: scanner.REGEX, lit: `foo/bar`},
		{in: `/foo\\/bar/`, tok: scanner.REGEX, lit: `foo\/bar`},
		{in: `/foo\\bar/`, tok: scanner.REGEX, lit: `foo\\bar`},
		{in: `/http\:\/\/www\.example\.com/`, tok: scanner.REGEX, lit: `http\://www\.example\.com`},
	}

	for i, tt := range tests {
		s := scanner.NewScanner(strings.NewReader(tt.in))
		tok, _, lit := s.ScanRegex()
		if tok != tt.tok {
			t.Errorf("%d. %s: error:\n\texp=%s\n\tgot=%s\n", i, tt.in, tt.tok.String(), tok.String())
		}
		if lit != tt.lit {
			t.Errorf("%d. %s: error:\n\texp=%s\n\tgot=%s\n", i, tt.in, tt.lit, lit)
		}
	}
}
