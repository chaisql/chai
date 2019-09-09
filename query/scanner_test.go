package query_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/asdine/genji/query"
)

// Ensure the scanner can scan tokens correctly.
func TestScanner_Scan(t *testing.T) {
	var tests = []struct {
		s   string
		tok query.Token
		lit string
		pos query.Pos
	}{
		// Special tokens (EOF, ILLEGAL, WS)
		{s: ``, tok: query.EOF},
		{s: `#`, tok: query.ILLEGAL, lit: `#`},
		{s: ` `, tok: query.WS, lit: " "},
		{s: "\t", tok: query.WS, lit: "\t"},
		{s: "\n", tok: query.WS, lit: "\n"},
		{s: "\r", tok: query.WS, lit: "\n"},
		{s: "\r\n", tok: query.WS, lit: "\n"},
		{s: "\rX", tok: query.WS, lit: "\n"},
		{s: "\n\r", tok: query.WS, lit: "\n\n"},
		{s: " \n\t \r\n\t", tok: query.WS, lit: " \n\t \n\t"},
		{s: " foo", tok: query.WS, lit: " "},

		// Numeric operators
		{s: `+`, tok: query.ADD},
		{s: `-`, tok: query.SUB},
		{s: `*`, tok: query.MUL},
		{s: `/`, tok: query.DIV},
		{s: `%`, tok: query.MOD},

		// Logical operators
		{s: `AND`, tok: query.AND},
		{s: `and`, tok: query.AND},
		{s: `OR`, tok: query.OR},
		{s: `or`, tok: query.OR},

		{s: `=`, tok: query.EQ},
		{s: `<>`, tok: query.NEQ},
		{s: `! `, tok: query.ILLEGAL, lit: "!"},
		{s: `<`, tok: query.LT},
		{s: `<=`, tok: query.LTE},
		{s: `>`, tok: query.GT},
		{s: `>=`, tok: query.GTE},

		// Misc tokens
		{s: `(`, tok: query.LPAREN},
		{s: `)`, tok: query.RPAREN},
		{s: `,`, tok: query.COMMA},
		{s: `;`, tok: query.SEMICOLON},
		{s: `.`, tok: query.DOT},
		{s: `=~`, tok: query.EQREGEX},
		{s: `!~`, tok: query.NEQREGEX},
		{s: `:`, tok: query.COLON},
		{s: `::`, tok: query.DOUBLECOLON},

		// Identifiers
		{s: `foo`, tok: query.IDENT, lit: `foo`},
		{s: `_foo`, tok: query.IDENT, lit: `_foo`},
		{s: `Zx12_3U_-`, tok: query.IDENT, lit: `Zx12_3U_`},
		{s: `"foo"`, tok: query.IDENT, lit: `foo`},
		{s: `"foo\\bar"`, tok: query.IDENT, lit: `foo\bar`},
		{s: `"foo\bar"`, tok: query.BADESCAPE, lit: `\b`, pos: query.Pos{Line: 0, Char: 5}},
		{s: `"foo\"bar\""`, tok: query.IDENT, lit: `foo"bar"`},
		{s: `test"`, tok: query.BADSTRING, lit: "", pos: query.Pos{Line: 0, Char: 3}},
		{s: `"test`, tok: query.BADSTRING, lit: `test`},
		{s: `$host`, tok: query.BOUNDPARAM, lit: `$host`},
		{s: `$"host param"`, tok: query.BOUNDPARAM, lit: `$host param`},

		{s: `true`, tok: query.TRUE},
		{s: `false`, tok: query.FALSE},

		// Strings
		{s: `'testing 123!'`, tok: query.STRING, lit: `testing 123!`},
		{s: `'foo\nbar'`, tok: query.STRING, lit: "foo\nbar"},
		{s: `'foo\\bar'`, tok: query.STRING, lit: "foo\\bar"},
		{s: `'test`, tok: query.BADSTRING, lit: `test`},
		{s: "'test\nfoo", tok: query.BADSTRING, lit: `test`},
		{s: `'test\g'`, tok: query.BADESCAPE, lit: `\g`, pos: query.Pos{Line: 0, Char: 6}},

		// Numbers
		{s: `100`, tok: query.INTEGER, lit: `100`},
		{s: `100.23`, tok: query.NUMBER, lit: `100.23`},
		{s: `.23`, tok: query.NUMBER, lit: `.23`},
		//{s: `.`, tok: query.ILLEGAL, lit: `.`},
		{s: `10.3s`, tok: query.NUMBER, lit: `10.3`},

		// Durations
		{s: `10u`, tok: query.DURATIONVAL, lit: `10u`},
		{s: `10µ`, tok: query.DURATIONVAL, lit: `10µ`},
		{s: `10ms`, tok: query.DURATIONVAL, lit: `10ms`},
		{s: `1s`, tok: query.DURATIONVAL, lit: `1s`},
		{s: `10m`, tok: query.DURATIONVAL, lit: `10m`},
		{s: `10h`, tok: query.DURATIONVAL, lit: `10h`},
		{s: `10d`, tok: query.DURATIONVAL, lit: `10d`},
		{s: `10w`, tok: query.DURATIONVAL, lit: `10w`},
		{s: `10x`, tok: query.DURATIONVAL, lit: `10x`}, // non-duration unit, but scanned as a duration value

		// Keywords
		{s: `ALL`, tok: query.ALL},
		{s: `ALTER`, tok: query.ALTER},
		{s: `AS`, tok: query.AS},
		{s: `ASC`, tok: query.ASC},
		{s: `BY`, tok: query.BY},
		{s: `DELETE`, tok: query.DELETE},
		{s: `DESC`, tok: query.DESC},
		{s: `DROP`, tok: query.DROP},
		{s: `DURATION`, tok: query.DURATION},
		{s: `FROM`, tok: query.FROM},
		{s: `INSERT`, tok: query.INSERT},
		{s: `INTO`, tok: query.INTO},
		{s: `LIMIT`, tok: query.LIMIT},
		{s: `OFFSET`, tok: query.OFFSET},
		{s: `ORDER`, tok: query.ORDER},
		{s: `SELECT`, tok: query.SELECT},
		{s: `TO`, tok: query.TO},
		{s: `VALUES`, tok: query.VALUES},
		{s: `WHERE`, tok: query.WHERE},
		{s: `seLECT`, tok: query.SELECT}, // case insensitive
	}

	for i, tt := range tests {
		s := query.NewScanner(strings.NewReader(tt.s))
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
		tok query.Token
		pos query.Pos
		lit string
	}
	exp := []result{
		{tok: query.SELECT, pos: query.Pos{Line: 0, Char: 0}, lit: ""},
		{tok: query.WS, pos: query.Pos{Line: 0, Char: 6}, lit: " "},
		{tok: query.IDENT, pos: query.Pos{Line: 0, Char: 7}, lit: "value"},
		{tok: query.WS, pos: query.Pos{Line: 0, Char: 12}, lit: " "},
		{tok: query.FROM, pos: query.Pos{Line: 0, Char: 13}, lit: ""},
		{tok: query.WS, pos: query.Pos{Line: 0, Char: 17}, lit: " "},
		{tok: query.IDENT, pos: query.Pos{Line: 0, Char: 18}, lit: "myseries"},
		{tok: query.WS, pos: query.Pos{Line: 0, Char: 26}, lit: " "},
		{tok: query.WHERE, pos: query.Pos{Line: 0, Char: 27}, lit: ""},
		{tok: query.WS, pos: query.Pos{Line: 0, Char: 32}, lit: " "},
		{tok: query.IDENT, pos: query.Pos{Line: 0, Char: 33}, lit: "a"},
		{tok: query.WS, pos: query.Pos{Line: 0, Char: 34}, lit: " "},
		{tok: query.EQ, pos: query.Pos{Line: 0, Char: 35}, lit: ""},
		{tok: query.WS, pos: query.Pos{Line: 0, Char: 36}, lit: " "},
		{tok: query.STRING, pos: query.Pos{Line: 0, Char: 36}, lit: "b"},
		{tok: query.EOF, pos: query.Pos{Line: 0, Char: 40}, lit: ""},
	}

	// Create a scanner.
	v := `SELECT value from myseries WHERE a = 'b'`
	s := query.NewScanner(strings.NewReader(v))

	// Continually scan until we reach the end.
	var act []result
	for {
		tok, pos, lit := s.Scan()
		act = append(act, result{tok, pos, lit})
		if tok == query.EOF {
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
		out, err := query.ScanString(strings.NewReader(tt.in))
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
		tok query.Token
		lit string
		err string
	}{
		{in: `/^payments\./`, tok: query.REGEX, lit: `^payments\.`},
		{in: `/foo\/bar/`, tok: query.REGEX, lit: `foo/bar`},
		{in: `/foo\\/bar/`, tok: query.REGEX, lit: `foo\/bar`},
		{in: `/foo\\bar/`, tok: query.REGEX, lit: `foo\\bar`},
		{in: `/http\:\/\/www\.example\.com/`, tok: query.REGEX, lit: `http\://www\.example\.com`},
	}

	for i, tt := range tests {
		s := query.NewScanner(strings.NewReader(tt.in))
		tok, _, lit := s.ScanRegex()
		if tok != tt.tok {
			t.Errorf("%d. %s: error:\n\texp=%s\n\tgot=%s\n", i, tt.in, tt.tok.String(), tok.String())
		}
		if lit != tt.lit {
			t.Errorf("%d. %s: error:\n\texp=%s\n\tgot=%s\n", i, tt.in, tt.lit, lit)
		}
	}
}
