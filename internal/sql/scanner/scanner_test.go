package scanner

import (
	"reflect"
	"strings"
	"testing"
)

// Ensure the scanner can scan tokens correctly.
func TestScanner_Scan(t *testing.T) {
	var tests = []struct {
		s   string
		tok Token
		lit string
		pos Pos
	}{
		// Special tokens (EOF, ILLEGAL, WS)
		{s: ``, tok: EOF},
		{s: `#`, tok: ILLEGAL, lit: `#`},
		{s: ` `, tok: WS, lit: " "},
		{s: "\t", tok: WS, lit: "\t"},
		{s: "\n", tok: WS, lit: "\n"},
		{s: "\r", tok: WS, lit: "\n"},
		{s: "\r\n", tok: WS, lit: "\n"},
		{s: "\rX", tok: WS, lit: "\n"},
		{s: "\n\r", tok: WS, lit: "\n\n"},
		{s: " \n\t \r\n\t", tok: WS, lit: " \n\t \n\t"},
		{s: " foo", tok: WS, lit: " "},
		{s: "...", tok: ELLIPSIS, lit: "..."},

		// Numeric operators
		{s: `+`, tok: ADD},
		{s: `-`, tok: SUB},
		{s: `*`, tok: MUL},
		{s: `/`, tok: DIV},
		{s: `%`, tok: MOD},

		// Logical operators
		{s: `AND`, tok: AND},
		{s: `and`, tok: AND},
		{s: `OR`, tok: OR},
		{s: `or`, tok: OR},

		// Comparison operators
		{s: `=`, tok: EQ},
		{s: `==`, tok: EQ},
		{s: `<>`, tok: NEQ},
		{s: `! `, tok: ILLEGAL, lit: "!"},
		{s: `<`, tok: LT},
		{s: `<=`, tok: LTE},
		{s: `>`, tok: GT},
		{s: `>=`, tok: GTE},
		{s: `IN`, tok: IN},
		{s: `IS`, tok: IS},
		{s: `LIKE`, tok: LIKE},
		{s: `||`, tok: CONCAT},

		// Misc tokens
		{s: `(`, tok: LPAREN},
		{s: `)`, tok: RPAREN},
		{s: `{`, tok: LBRACKET},
		{s: `}`, tok: RBRACKET},
		{s: `[`, tok: LSBRACKET},
		{s: `]`, tok: RSBRACKET},
		{s: `,`, tok: COMMA},
		{s: `;`, tok: SEMICOLON},
		{s: `.`, tok: DOT},
		{s: `=~`, tok: EQREGEX},
		{s: `!~`, tok: NEQREGEX},
		{s: `:`, tok: COLON},
		{s: `::`, tok: DOUBLECOLON},
		{s: `--`, tok: COMMENT},
		{s: `--10.3`, tok: COMMENT, lit: ``},

		// Identifiers
		{s: `foo`, tok: IDENT, lit: `foo`},
		{s: `_foo`, tok: IDENT, lit: `_foo`},
		{s: `Zx12_3U_-`, tok: IDENT, lit: `Zx12_3U_`},
		{s: "`foo`", tok: IDENT, lit: "foo"},
		{s: "`foo\bar`", tok: IDENT, lit: "foo\bar"},
		{s: "`foo\\bar`", tok: BADESCAPE, lit: `\b`, pos: Pos{Line: 0, Char: 5}},
		{s: "`foo\\`bar\\``", tok: IDENT, lit: "foo`bar`"},
		{s: "test`", tok: BADSTRING, lit: "", pos: Pos{Line: 0, Char: 3}},
		{s: "`test", tok: BADSTRING, lit: "test"},
		{s: "$host", tok: NAMEDPARAM, lit: "$host"},
		{s: "$`host param`", tok: NAMEDPARAM, lit: "$host param"},
		{s: "?", tok: POSITIONALPARAM, lit: ""},

		// Booleans
		{s: `true`, tok: TRUE},
		{s: `false`, tok: FALSE},

		// Null
		{s: `null`, tok: NULL},
		{s: `NULL`, tok: NULL},

		// Strings
		{s: `'testing 123!'`, tok: STRING, lit: `testing 123!`},
		{s: `'foo\nbar'`, tok: STRING, lit: "foo\nbar"},
		{s: `'foo\\bar'`, tok: STRING, lit: "foo\\bar"},
		{s: `'test`, tok: BADSTRING, lit: `test`},
		{s: "'test\nfoo", tok: BADSTRING, lit: `test`},
		{s: `'test\g'`, tok: BADESCAPE, lit: `\g`, pos: Pos{Line: 0, Char: 6}},
		{s: `"testing 123!"`, tok: STRING, lit: `testing 123!`},
		{s: `"foo\nbar"`, tok: STRING, lit: "foo\nbar"},
		{s: `"foo\\bar"`, tok: STRING, lit: "foo\\bar"},
		{s: `"test`, tok: BADSTRING, lit: `test`},
		{s: "\"test\nfoo", tok: BADSTRING, lit: `test`},
		{s: `"test\g"`, tok: BADESCAPE, lit: `\g`, pos: Pos{Line: 0, Char: 6}},

		// Numbers
		{s: `100`, tok: INTEGER, lit: `100`},
		{s: `100.23`, tok: NUMBER, lit: `100.23`},
		{s: `.23`, tok: NUMBER, lit: `.23`},
		{s: `10.3s`, tok: NUMBER, lit: `10.3`},
		{s: `1.2e10`, tok: NUMBER, lit: `1.2e10`},
		{s: `1.2E10`, tok: NUMBER, lit: `1.2E10`},
		{s: `1.2e+10`, tok: NUMBER, lit: `1.2e+10`},
		{s: `1.2e-10`, tok: NUMBER, lit: `1.2e-10`},

		// Keywords
		{s: `ADD`, tok: ADD_KEYWORD},
		{s: `ALTER`, tok: ALTER},
		{s: `AS`, tok: AS},
		{s: `ASC`, tok: ASC},
		{s: `ALL`, tok: ALL},
		{s: `BY`, tok: BY},
		{s: `BEGIN`, tok: BEGIN},
		{s: `BETWEEN`, tok: BETWEEN},
		{s: `CACHE`, tok: CACHE},
		{s: `CAST`, tok: CAST},
		{s: `CHECK`, tok: CHECK},
		{s: `COMMIT`, tok: COMMIT},
		{s: `CONFLICT`, tok: CONFLICT},
		{s: `CONSTRAINT`, tok: CONSTRAINT},
		{s: `CREATE`, tok: CREATE},
		{s: `CYCLE`, tok: CYCLE},
		{s: `DEFAULT`, tok: DEFAULT},
		{s: `DELETE`, tok: DELETE},
		{s: `DESC`, tok: DESC},
		{s: `DO`, tok: DO},
		{s: `DISTINCT`, tok: DISTINCT},
		{s: `DROP`, tok: DROP},
		{s: `EXPLAIN`, tok: EXPLAIN},
		{s: `GROUP`, tok: GROUP},
		{s: `COLUMN`, tok: COLUMN},
		{s: `FOR`, tok: FOR},
		{s: `FROM`, tok: FROM},
		{s: `IGNORE`, tok: IGNORE},
		{s: `INCREMENT`, tok: INCREMENT},
		{s: `INDEX`, tok: INDEX},
		{s: `INSERT`, tok: INSERT},
		{s: `INTO`, tok: INTO},
		{s: `LIMIT`, tok: LIMIT},
		{s: `MAXVALUE`, tok: MAXVALUE},
		{s: `MINVALUE`, tok: MINVALUE},
		{s: `NEXT`, tok: NEXT},
		{s: `NO`, tok: NO},
		{s: `NOT`, tok: NOT},
		{s: `NOTHING`, tok: NOTHING},
		{s: `ONLY`, tok: ONLY},
		{s: `OFFSET`, tok: OFFSET},
		{s: `ORDER`, tok: ORDER},
		{s: `PRIMARY`, tok: PRIMARY},
		{s: `READ`, tok: READ},
		{s: `REINDEX`, tok: REINDEX},
		{s: `RENAME`, tok: RENAME},
		{s: `REPLACE`, tok: REPLACE},
		{s: `RETURNING`, tok: RETURNING},
		{s: `ROLLBACK`, tok: ROLLBACK},
		{s: `SELECT`, tok: SELECT},
		{s: `SEQUENCE`, tok: SEQUENCE},
		{s: `SET`, tok: SET},
		{s: `START`, tok: START},
		{s: `TABLE`, tok: TABLE},
		{s: `TO`, tok: TO},
		{s: `TRANSACTION`, tok: TRANSACTION},
		{s: `UPDATE`, tok: UPDATE},
		{s: `UNION`, tok: UNION},
		{s: `UNSET`, tok: UNSET},
		{s: `VALUE`, tok: VALUE},
		{s: `VALUES`, tok: VALUES},
		{s: `WITH`, tok: WITH},
		{s: `WHERE`, tok: WHERE},
		{s: `WRITE`, tok: WRITE},
		{s: `seLECT`, tok: SELECT}, // case insensitive

		// types
		{s: "ANY", tok: TYPEANY},
		{s: "BYTES", tok: TYPEBYTES},
		{s: "BOOL", tok: TYPEBOOL},
		{s: "BOOLEAN", tok: TYPEBOOLEAN},
		{s: "DOUBLE", tok: TYPEDOUBLE},
		{s: "INTEGER", tok: TYPEINTEGER},
		{s: "TEXT", tok: TYPETEXT},
		{s: "TIMESTAMP", tok: TYPETIMESTAMP},
		{s: "OBJECT", tok: TYPEOBJECT},
	}

	for i, tt := range tests {
		s := NewScanner(strings.NewReader(tt.s))
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
		tok Token
		pos Pos
		lit string
	}

	exp := []result{
		{tok: SELECT, pos: Pos{Line: 0, Char: 0}, lit: ""},
		{tok: WS, pos: Pos{Line: 0, Char: 6}, lit: " "},
		{tok: IDENT, pos: Pos{Line: 0, Char: 7}, lit: "val"},
		{tok: WS, pos: Pos{Line: 0, Char: 10}, lit: " "},
		{tok: FROM, pos: Pos{Line: 0, Char: 11}, lit: ""},
		{tok: WS, pos: Pos{Line: 0, Char: 15}, lit: " "},
		{tok: IDENT, pos: Pos{Line: 0, Char: 16}, lit: "my_table"},
		{tok: WS, pos: Pos{Line: 0, Char: 24}, lit: " "},
		{tok: WHERE, pos: Pos{Line: 0, Char: 25}, lit: ""},
		{tok: WS, pos: Pos{Line: 0, Char: 30}, lit: " "},
		{tok: IDENT, pos: Pos{Line: 0, Char: 31}, lit: "a"},
		{tok: WS, pos: Pos{Line: 0, Char: 32}, lit: " "},
		{tok: EQ, pos: Pos{Line: 0, Char: 33}, lit: ""},
		{tok: WS, pos: Pos{Line: 0, Char: 34}, lit: " "},
		{tok: STRING, pos: Pos{Line: 0, Char: 34}, lit: "b"},
		{tok: EOF, pos: Pos{Line: 0, Char: 38}, lit: ""},
	}

	v := `SELECT val from my_table WHERE a = 'b'`
	s := newScanner(strings.NewReader(v))

	// Continually scan until we reach the end.
	var act []result
	for {
		tok, pos, lit := s.Scan()
		act = append(act, result{tok, pos, lit})
		if tok == EOF {
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
		{in: `"foo\rbar"`, out: "foo\rbar"},
		{in: `"foo\tbar"`, out: "foo\tbar"},
		{in: `"foo\r\nbar"`, out: "foo\r\nbar"},
		{in: `"foo\r\nbar\r\n\trm"`, out: "foo\r\nbar\r\n\trm"},
		{in: `"foo\\bar"`, out: `foo\bar`},
		{in: `"foo\"bar"`, out: `foo"bar`},
		{in: `'foo\'bar'`, out: `foo'bar`},
		{in: `'\xAF'`, out: `\xAF`},

		{in: `"foo` + "\n", out: `foo`, err: "bad string"}, // newline in string
		{in: `"foo`, out: `foo`, err: "bad string"},        // unclosed quotes
		{in: `"foo\xbar"`, out: `\x`, err: "bad escape"},   // invalid escape
	}

	for i, tt := range tests {
		out, err := scanString(strings.NewReader(tt.in))
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
		tok Token
		lit string
	}{
		{in: `/^payments\./`, tok: REGEX, lit: `^payments\.`},
		{in: `/foo\/bar/`, tok: REGEX, lit: `foo/bar`},
		{in: `/foo\\/bar/`, tok: REGEX, lit: `foo\/bar`},
		{in: `/foo\\bar/`, tok: REGEX, lit: `foo\\bar`},
		{in: `/http\:\/\/www\.example\.com/`, tok: REGEX, lit: `http\://www\.example\.com`},
	}

	for i, tt := range tests {
		s := newScanner(strings.NewReader(tt.in))
		tok, _, lit := s.ScanRegex()
		if tok != tt.tok {
			t.Errorf("%d. %s: error:\n\texp=%s\n\tgot=%s\n", i, tt.in, tt.tok.String(), tok.String())
		}
		if lit != tt.lit {
			t.Errorf("%d. %s: error:\n\texp=%s\n\tgot=%s\n", i, tt.in, tt.lit, lit)
		}
	}
}
