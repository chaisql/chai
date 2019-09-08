package sql_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/asdine/genji/query/sql"
)

// Ensure the scanner can scan tokens correctly.
func TestScanner_Scan(t *testing.T) {
	var tests = []struct {
		s   string
		tok sql.Token
		lit string
		pos sql.Pos
	}{
		// Special tokens (EOF, ILLEGAL, WS)
		{s: ``, tok: sql.EOF},
		{s: `#`, tok: sql.ILLEGAL, lit: `#`},
		{s: ` `, tok: sql.WS, lit: " "},
		{s: "\t", tok: sql.WS, lit: "\t"},
		{s: "\n", tok: sql.WS, lit: "\n"},
		{s: "\r", tok: sql.WS, lit: "\n"},
		{s: "\r\n", tok: sql.WS, lit: "\n"},
		{s: "\rX", tok: sql.WS, lit: "\n"},
		{s: "\n\r", tok: sql.WS, lit: "\n\n"},
		{s: " \n\t \r\n\t", tok: sql.WS, lit: " \n\t \n\t"},
		{s: " foo", tok: sql.WS, lit: " "},

		// Numeric operators
		{s: `+`, tok: sql.ADD},
		{s: `-`, tok: sql.SUB},
		{s: `*`, tok: sql.MUL},
		{s: `/`, tok: sql.DIV},
		{s: `%`, tok: sql.MOD},

		// Logical operators
		{s: `AND`, tok: sql.AND},
		{s: `and`, tok: sql.AND},
		{s: `OR`, tok: sql.OR},
		{s: `or`, tok: sql.OR},

		{s: `=`, tok: sql.EQ},
		{s: `<>`, tok: sql.NEQ},
		{s: `! `, tok: sql.ILLEGAL, lit: "!"},
		{s: `<`, tok: sql.LT},
		{s: `<=`, tok: sql.LTE},
		{s: `>`, tok: sql.GT},
		{s: `>=`, tok: sql.GTE},

		// Misc tokens
		{s: `(`, tok: sql.LPAREN},
		{s: `)`, tok: sql.RPAREN},
		{s: `,`, tok: sql.COMMA},
		{s: `;`, tok: sql.SEMICOLON},
		{s: `.`, tok: sql.DOT},
		{s: `=~`, tok: sql.EQREGEX},
		{s: `!~`, tok: sql.NEQREGEX},
		{s: `:`, tok: sql.COLON},
		{s: `::`, tok: sql.DOUBLECOLON},

		// Identifiers
		{s: `foo`, tok: sql.IDENT, lit: `foo`},
		{s: `_foo`, tok: sql.IDENT, lit: `_foo`},
		{s: `Zx12_3U_-`, tok: sql.IDENT, lit: `Zx12_3U_`},
		{s: `"foo"`, tok: sql.IDENT, lit: `foo`},
		{s: `"foo\\bar"`, tok: sql.IDENT, lit: `foo\bar`},
		{s: `"foo\bar"`, tok: sql.BADESCAPE, lit: `\b`, pos: sql.Pos{Line: 0, Char: 5}},
		{s: `"foo\"bar\""`, tok: sql.IDENT, lit: `foo"bar"`},
		{s: `test"`, tok: sql.BADSTRING, lit: "", pos: sql.Pos{Line: 0, Char: 3}},
		{s: `"test`, tok: sql.BADSTRING, lit: `test`},
		{s: `$host`, tok: sql.BOUNDPARAM, lit: `$host`},
		{s: `$"host param"`, tok: sql.BOUNDPARAM, lit: `$host param`},

		{s: `true`, tok: sql.TRUE},
		{s: `false`, tok: sql.FALSE},

		// Strings
		{s: `'testing 123!'`, tok: sql.STRING, lit: `testing 123!`},
		{s: `'foo\nbar'`, tok: sql.STRING, lit: "foo\nbar"},
		{s: `'foo\\bar'`, tok: sql.STRING, lit: "foo\\bar"},
		{s: `'test`, tok: sql.BADSTRING, lit: `test`},
		{s: "'test\nfoo", tok: sql.BADSTRING, lit: `test`},
		{s: `'test\g'`, tok: sql.BADESCAPE, lit: `\g`, pos: sql.Pos{Line: 0, Char: 6}},

		// Numbers
		{s: `100`, tok: sql.INTEGER, lit: `100`},
		{s: `100.23`, tok: sql.NUMBER, lit: `100.23`},
		{s: `.23`, tok: sql.NUMBER, lit: `.23`},
		//{s: `.`, tok: sql.ILLEGAL, lit: `.`},
		{s: `10.3s`, tok: sql.NUMBER, lit: `10.3`},

		// Durations
		{s: `10u`, tok: sql.DURATIONVAL, lit: `10u`},
		{s: `10µ`, tok: sql.DURATIONVAL, lit: `10µ`},
		{s: `10ms`, tok: sql.DURATIONVAL, lit: `10ms`},
		{s: `1s`, tok: sql.DURATIONVAL, lit: `1s`},
		{s: `10m`, tok: sql.DURATIONVAL, lit: `10m`},
		{s: `10h`, tok: sql.DURATIONVAL, lit: `10h`},
		{s: `10d`, tok: sql.DURATIONVAL, lit: `10d`},
		{s: `10w`, tok: sql.DURATIONVAL, lit: `10w`},
		{s: `10x`, tok: sql.DURATIONVAL, lit: `10x`}, // non-duration unit, but scanned as a duration value

		// Keywords
		{s: `ALL`, tok: sql.ALL},
		{s: `ALTER`, tok: sql.ALTER},
		{s: `AS`, tok: sql.AS},
		{s: `ASC`, tok: sql.ASC},
		{s: `BY`, tok: sql.BY},
		{s: `DELETE`, tok: sql.DELETE},
		{s: `DESC`, tok: sql.DESC},
		{s: `DROP`, tok: sql.DROP},
		{s: `DURATION`, tok: sql.DURATION},
		{s: `FROM`, tok: sql.FROM},
		{s: `INSERT`, tok: sql.INSERT},
		{s: `INTO`, tok: sql.INTO},
		{s: `LIMIT`, tok: sql.LIMIT},
		{s: `OFFSET`, tok: sql.OFFSET},
		{s: `ORDER`, tok: sql.ORDER},
		{s: `SELECT`, tok: sql.SELECT},
		{s: `TO`, tok: sql.TO},
		{s: `VALUES`, tok: sql.VALUES},
		{s: `WHERE`, tok: sql.WHERE},
		{s: `seLECT`, tok: sql.SELECT}, // case insensitive
	}

	for i, tt := range tests {
		s := sql.NewScanner(strings.NewReader(tt.s))
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
		tok sql.Token
		pos sql.Pos
		lit string
	}
	exp := []result{
		{tok: sql.SELECT, pos: sql.Pos{Line: 0, Char: 0}, lit: ""},
		{tok: sql.WS, pos: sql.Pos{Line: 0, Char: 6}, lit: " "},
		{tok: sql.IDENT, pos: sql.Pos{Line: 0, Char: 7}, lit: "value"},
		{tok: sql.WS, pos: sql.Pos{Line: 0, Char: 12}, lit: " "},
		{tok: sql.FROM, pos: sql.Pos{Line: 0, Char: 13}, lit: ""},
		{tok: sql.WS, pos: sql.Pos{Line: 0, Char: 17}, lit: " "},
		{tok: sql.IDENT, pos: sql.Pos{Line: 0, Char: 18}, lit: "myseries"},
		{tok: sql.WS, pos: sql.Pos{Line: 0, Char: 26}, lit: " "},
		{tok: sql.WHERE, pos: sql.Pos{Line: 0, Char: 27}, lit: ""},
		{tok: sql.WS, pos: sql.Pos{Line: 0, Char: 32}, lit: " "},
		{tok: sql.IDENT, pos: sql.Pos{Line: 0, Char: 33}, lit: "a"},
		{tok: sql.WS, pos: sql.Pos{Line: 0, Char: 34}, lit: " "},
		{tok: sql.EQ, pos: sql.Pos{Line: 0, Char: 35}, lit: ""},
		{tok: sql.WS, pos: sql.Pos{Line: 0, Char: 36}, lit: " "},
		{tok: sql.STRING, pos: sql.Pos{Line: 0, Char: 36}, lit: "b"},
		{tok: sql.EOF, pos: sql.Pos{Line: 0, Char: 40}, lit: ""},
	}

	// Create a scanner.
	v := `SELECT value from myseries WHERE a = 'b'`
	s := sql.NewScanner(strings.NewReader(v))

	// Continually scan until we reach the end.
	var act []result
	for {
		tok, pos, lit := s.Scan()
		act = append(act, result{tok, pos, lit})
		if tok == sql.EOF {
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
		out, err := sql.ScanString(strings.NewReader(tt.in))
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
		tok sql.Token
		lit string
		err string
	}{
		{in: `/^payments\./`, tok: sql.REGEX, lit: `^payments\.`},
		{in: `/foo\/bar/`, tok: sql.REGEX, lit: `foo/bar`},
		{in: `/foo\\/bar/`, tok: sql.REGEX, lit: `foo\/bar`},
		{in: `/foo\\bar/`, tok: sql.REGEX, lit: `foo\\bar`},
		{in: `/http\:\/\/www\.example\.com/`, tok: sql.REGEX, lit: `http\://www\.example\.com`},
	}

	for i, tt := range tests {
		s := sql.NewScanner(strings.NewReader(tt.in))
		tok, _, lit := s.ScanRegex()
		if tok != tt.tok {
			t.Errorf("%d. %s: error:\n\texp=%s\n\tgot=%s\n", i, tt.in, tt.tok.String(), tok.String())
		}
		if lit != tt.lit {
			t.Errorf("%d. %s: error:\n\texp=%s\n\tgot=%s\n", i, tt.in, tt.lit, lit)
		}
	}
}
