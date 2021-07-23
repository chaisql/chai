package scanner

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strings"

	"github.com/genjidb/genji/internal/stringutil"
)

// Code heavily inspired by the influxdata/influxql repository
// https://github.com/influxdata/influxql/blob/57f403b00b124eb900835c0c944e9b60d848db5e/scanner.go#L12

func init() {
	keywords = make(map[string]Token)
	for tok := keywordBeg + 1; tok < keywordEnd; tok++ {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
	for _, tok := range []Token{AND, OR, TRUE, FALSE, NULL, IN, IS, LIKE, BETWEEN} {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
}

// scanner represents a lexical scanner for Genji.
type scanner struct {
	r *reader
}

// newScanner returns a new instance of Scanner.
func newScanner(r io.Reader) *scanner {
	return &scanner{r: &reader{r: bufio.NewReaderSize(r, 128)}}
}

// Scan returns the next token and position from the underlying reader.
// Also returns the literal text read for strings, and number tokens
// since these token types can have different literal representations.
func (s *scanner) Scan() (tok Token, pos Pos, lit string) {
	// Read next code point.
	ch0, pos := s.r.read()

	// If we see whitespace then consume all contiguous whitespace.
	// If we see a letter, or certain acceptable special characters, then consume
	// as an ident or reserved word.
	if isWhitespace(ch0) {
		return s.scanWhitespace()
	} else if isLetter(ch0) || ch0 == '_' {
		s.r.unread()
		return s.scanIdent(true)
	} else if isDigit(ch0) {
		return s.scanNumber()
	}

	// Otherwise parse individual characters.
	switch ch0 {
	case eof:
		return EOF, pos, ""
	case '`':
		s.r.unread()
		return s.scanIdent(true)
	case '"':
		return s.scanString()
	case '\'':
		return s.scanString()
	case '.':
		ch1, _ := s.r.read()
		s.r.unread()
		if isDigit(ch1) {
			return s.scanNumber()
		}
		return DOT, pos, ""
	case '$':
		tok, _, lit := s.scanIdent(false)

		if tok != IDENT {
			return tok, pos, "$" + lit
		}
		return NAMEDPARAM, pos, "$" + lit
	case '?':
		return POSITIONALPARAM, pos, ""
	case '+':
		return ADD, pos, ""
	case '-':
		ch1, _ := s.r.read()
		if ch1 == '-' {
			s.skipUntilNewline()
			return COMMENT, pos, ""
		}
		if isDigit(ch1) {
			s.r.unread()
			return s.scanNumber()
		}
		s.r.unread()
		return SUB, pos, ""
	case '*':
		return MUL, pos, ""
	case '/':
		ch1, _ := s.r.read()
		if ch1 == '*' {
			if err := s.skipUntilEndComment(); err != nil {
				return ILLEGAL, pos, ""
			}
			return COMMENT, pos, ""
		}
		s.r.unread()
		return DIV, pos, ""
	case '%':
		return MOD, pos, ""
	case '&':
		return BITWISEAND, pos, ""
	case '|':
		ch1, _ := s.r.read()
		if ch1 == '|' {
			return CONCAT, pos, ""
		}
		s.r.unread()
		return BITWISEOR, pos, ""
	case '^':
		return BITWISEXOR, pos, ""
	case '=':
		ch1, _ := s.r.read()
		if ch1 == '~' {
			return EQREGEX, pos, ""
		}
		if ch1 == '=' {
			return EQ, pos, ""
		}
		s.r.unread()
		return EQ, pos, ""
	case '!':
		if ch1, _ := s.r.read(); ch1 == '=' {
			return NEQ, pos, ""
		} else if ch1 == '~' {
			return NEQREGEX, pos, ""
		}
		s.r.unread()
	case '>':
		if ch1, _ := s.r.read(); ch1 == '=' {
			return GTE, pos, ""
		}
		s.r.unread()
		return GT, pos, ""
	case '<':
		if ch1, _ := s.r.read(); ch1 == '=' {
			return LTE, pos, ""
		} else if ch1 == '>' {
			return NEQ, pos, ""
		}
		s.r.unread()
		return LT, pos, ""
	case '(':
		return LPAREN, pos, ""
	case ')':
		return RPAREN, pos, ""
	case '{':
		return LBRACKET, pos, ""
	case '}':
		return RBRACKET, pos, ""
	case '[':
		return LSBRACKET, pos, ""
	case ']':
		return RSBRACKET, pos, ""
	case ',':
		return COMMA, pos, ""
	case ';':
		return SEMICOLON, pos, ""
	case ':':
		if ch1, _ := s.r.read(); ch1 == ':' {
			return DOUBLECOLON, pos, ""
		}
		s.r.unread()
		return COLON, pos, ""
	}

	return ILLEGAL, pos, string(ch0)
}

// scanWhitespace consumes the current rune and all contiguous whitespace.
func (s *scanner) scanWhitespace() (tok Token, pos Pos, lit string) {
	// Create a buffer and read the current character into it.
	var buf bytes.Buffer
	ch, pos := s.r.curr()
	_, _ = buf.WriteRune(ch)

	// Read every subsequent whitespace character into the buffer.
	// Non-whitespace characters and EOF will cause the loop to exit.
	for {
		ch, _ = s.r.read()
		if ch == eof {
			break
		} else if !isWhitespace(ch) {
			s.r.unread()
			break
		} else {
			_, _ = buf.WriteRune(ch)
		}
	}

	return WS, pos, buf.String()
}

// skipUntilNewline skips characters until it reaches a newline.
func (s *scanner) skipUntilNewline() {
	for {
		if ch, _ := s.r.read(); ch == '\n' || ch == eof {
			return
		}
	}
}

// skipUntilEndComment skips characters until it reaches a '*/' symbol.
func (s *scanner) skipUntilEndComment() error {
	for {
		if ch1, _ := s.r.read(); ch1 == '*' {
			// We might be at the end.
		star:
			ch2, _ := s.r.read()
			if ch2 == '/' {
				return nil
			} else if ch2 == '*' {
				// We are back in the state machine since we see a star.
				goto star
			} else if ch2 == eof {
				return io.EOF
			}
		} else if ch1 == eof {
			return io.EOF
		}
	}
}

func (s *scanner) scanIdent(doLookup bool) (tok Token, pos Pos, lit string) {
	// Save the starting position of the identifier.
	_, pos = s.r.read()
	s.r.unread()

	var buf bytes.Buffer
	for {
		if ch, _ := s.r.read(); ch == eof {
			break
		} else if ch == '`' {
			tok0, pos0, lit0 := s.scanString()
			if tok0 == BADSTRING || tok0 == BADESCAPE {
				return tok0, pos0, lit0
			}
			return IDENT, pos, lit0
		} else if isIdentChar(ch) {
			s.r.unread()
			buf.WriteString(scanBareIdent(s.r))
		} else {
			s.r.unread()
			break
		}
	}
	lit = buf.String()

	// If the literal matches a keyword then return that keyword.
	if doLookup {
		if tok := lookup(lit); tok != IDENT {
			return tok, pos, ""
		}
	}
	return IDENT, pos, lit
}

// scanString consumes a contiguous string of non-quote characters.
// Quote characters can be consumed if they're first escaped with a backslash.
func (s *scanner) scanString() (tok Token, pos Pos, lit string) {
	s.r.unread()
	_, pos = s.r.curr()

	lit, err := scanString(s.r)

	if err == errBadString {
		return BADSTRING, pos, lit
	} else if err == errBadEscape {
		_, pos = s.r.curr()
		return BADESCAPE, pos, lit
	}
	return STRING, pos, lit
}

// ScanRegex consumes a token to find escapes
func (s *scanner) ScanRegex() (tok Token, pos Pos, lit string) {
	_, pos = s.r.curr()

	// Start & end sentinels.
	start, end := '/', '/'
	// Valid escape chars.
	escapes := map[rune]rune{'/': '/'}

	b, err := scanDelimited(s.r, start, end, escapes, true)

	if err == errBadEscape {
		_, pos = s.r.curr()
		return BADESCAPE, pos, ""
	} else if err != nil {
		return BADREGEX, pos, ""
	}
	return REGEX, pos, string(b)
}

// scanNumber consumes anything that looks like the start of a number.
func (s *scanner) scanNumber() (tok Token, pos Pos, lit string) {
	var buf bytes.Buffer

	// Check if the initial rune is a ".".
	ch, pos := s.r.curr()
	if ch == '.' {
		// Peek and see if the next rune is a digit.
		ch1, _ := s.r.read()
		s.r.unread()
		if !isDigit(ch1) {
			return ILLEGAL, pos, "."
		}

		// Unread the full stop so we can read it later.
		s.r.unread()
	} else if ch == '-' {
		buf.WriteRune(ch)
	} else {
		s.r.unread()
	}

	// Read as many digits as possible.
	_, _ = buf.WriteString(s.scanDigits())

	// If next code points are a full stop and digit then consume them.
	isDecimal := false
	if ch0, _ := s.r.read(); ch0 == '.' {
		isDecimal = true
		if ch1, _ := s.r.read(); isDigit(ch1) {
			_, _ = buf.WriteRune(ch0)
			_, _ = buf.WriteRune(ch1)
			_, _ = buf.WriteString(s.scanDigits())
		} else {
			s.r.unread()
		}
	} else {
		s.r.unread()
	}

	if !isDecimal {
		return INTEGER, pos, buf.String()
	}
	return NUMBER, pos, buf.String()
}

// scanDigits consumes a contiguous series of digits.
func (s *scanner) scanDigits() string {
	var buf bytes.Buffer
	for {
		ch, _ := s.r.read()
		if !isDigit(ch) {
			s.r.unread()
			break
		}
		_, _ = buf.WriteRune(ch)
	}
	return buf.String()
}

// isWhitespace returns true if the rune is a space, tab, or newline.
func isWhitespace(ch rune) bool { return ch == ' ' || ch == '\t' || ch == '\n' }

// isLetter returns true if the rune is a letter.
func isLetter(ch rune) bool { return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') }

// isDigit returns true if the rune is a digit.
func isDigit(ch rune) bool { return (ch >= '0' && ch <= '9') }

// isIdentChar returns true if the rune can be used in an unquoted identifier.
func isIdentChar(ch rune) bool { return isLetter(ch) || isDigit(ch) || ch == '_' }

// Scanner represents a buffered scanner.
// It provides a fixed-length circular buffer that can be unread.
type Scanner struct {
	s   *scanner
	i   int // buffer index
	n   int // buffer size
	buf [4]struct {
		tok Token
		pos Pos
		lit string
	}
}

// NewScanner returns a new buffered scanner for a reader.
func NewScanner(r io.Reader) *Scanner {
	return &Scanner{s: newScanner(r)}
}

// Scan reads the next token from the scanner.
func (s *Scanner) Scan() (tok Token, pos Pos, lit string) {
	return s.scanFunc(s.s.Scan)
}

// ScanRegex reads a regex token from the scanner.
func (s *Scanner) ScanRegex() (tok Token, pos Pos, lit string) {
	return s.scanFunc(s.s.ScanRegex)
}

// scanFunc uses the provided function to scan the next token.
func (s *Scanner) scanFunc(scan func() (Token, Pos, string)) (tok Token, pos Pos, lit string) {
	// If we have unread tokens then read them off the buffer first.
	if s.n > 0 {
		s.n--
		return s.Curr()
	}

	// Move buffer position forward and save the token.
	s.i = (s.i + 1) % len(s.buf)
	buf := &s.buf[s.i]
	buf.tok, buf.pos, buf.lit = scan()

	return s.Curr()
}

// Unscan pushes the previously token back onto the buffer.
func (s *Scanner) Unscan() { s.n++ }

// Curr returns the last read token.
func (s *Scanner) Curr() (tok Token, pos Pos, lit string) {
	buf := &s.buf[(s.i-s.n+len(s.buf))%len(s.buf)]
	return buf.tok, buf.pos, buf.lit
}

// reader represents a buffered rune reader used by the scanner.
// It provides a fixed-length circular buffer that can be unread.
type reader struct {
	r   io.RuneScanner
	i   int // buffer index
	n   int // buffer char count
	pos Pos // last read rune position
	buf [3]struct {
		ch  rune
		pos Pos
	}
	eof bool // true if reader has ever seen eof.
}

// ReadRune reads the next rune from the reader.
// This is a wrapper function to implement the io.RuneReader interface.
// Note that this function does not return size.
func (r *reader) ReadRune() (ch rune, size int, err error) {
	ch, _ = r.read()
	if ch == eof {
		err = io.EOF
	}
	return
}

// UnreadRune pushes the previously read rune back onto the buffer.
// This is a wrapper function to implement the io.RuneScanner interface.
func (r *reader) UnreadRune() error {
	r.unread()
	return nil
}

// read reads the next rune from the reader.
func (r *reader) read() (ch rune, pos Pos) {
	// If we have unread characters then read them off the buffer first.
	if r.n > 0 {
		r.n--
		return r.curr()
	}

	// Read next rune from underlying reader.
	// Any error (including io.EOF) should return as EOF.
	ch, _, err := r.r.ReadRune()
	if err != nil {
		ch = eof
	} else if ch == '\r' {
		if ch, _, err := r.r.ReadRune(); err != nil {
			// nop
		} else if ch != '\n' {
			_ = r.r.UnreadRune()
		}
		ch = '\n'
	}

	// Save character and position to the buffer.
	r.i = (r.i + 1) % len(r.buf)
	buf := &r.buf[r.i]
	buf.ch, buf.pos = ch, r.pos

	// Update position.
	// Only count EOF once.
	if ch == '\n' {
		r.pos.Line++
		r.pos.Char = 0
	} else if !r.eof {
		r.pos.Char++
	}

	// Mark the reader as EOF.
	// This is used so we don't double count EOF characters.
	if ch == eof {
		r.eof = true
	}

	return r.curr()
}

// unread pushes the previously read rune back onto the buffer.
func (r *reader) unread() {
	r.n++
}

// curr returns the last read character and position.
func (r *reader) curr() (ch rune, pos Pos) {
	i := (r.i - r.n + len(r.buf)) % len(r.buf)
	buf := &r.buf[i]
	return buf.ch, buf.pos
}

// eof is a marker code point to signify that the reader can't read any more.
const eof = rune(0)

// scanDelimited reads a delimited set of runes
func scanDelimited(r io.RuneScanner, start, end rune, escapes map[rune]rune, escapesPassThru bool) ([]byte, error) {
	// Scan start delimiter.
	if ch, _, err := r.ReadRune(); err != nil {
		return nil, err
	} else if ch != start {
		return nil, stringutil.Errorf("expected %s; found %s", string(start), string(ch))
	}

	var buf bytes.Buffer
	for {
		ch0, _, err := r.ReadRune()
		if ch0 == end {
			return buf.Bytes(), nil
		} else if err != nil {
			return buf.Bytes(), err
		} else if ch0 == '\n' {
			return nil, errors.New("delimited text contains new line")
		} else if ch0 == '\\' {
			// If the next character is an escape then write the escaped char.
			// If it's not a valid escape then return an error.
			ch1, _, err := r.ReadRune()
			if err != nil {
				return nil, err
			}

			c, ok := escapes[ch1]
			if !ok {
				if escapesPassThru {
					// Unread ch1 (char after the \)
					_ = r.UnreadRune()
					// Write ch0 (\) to the output buffer.
					_, _ = buf.WriteRune(ch0)
					continue
				} else {
					buf.Reset()
					_, _ = buf.WriteRune(ch0)
					_, _ = buf.WriteRune(ch1)
					return buf.Bytes(), errBadEscape
				}
			}

			_, _ = buf.WriteRune(c)
		} else {
			_, _ = buf.WriteRune(ch0)
		}
	}
}

// scanString reads a quoted string from a rune reader.
func scanString(r io.RuneReader) (string, error) {
	ending, _, err := r.ReadRune()
	if err != nil {
		return "", errBadString
	}

	var buf bytes.Buffer
	for i := 0; ; i++ {
		ch0, _, err := r.ReadRune()
		if ch0 == ending {
			return buf.String(), nil
		} else if err != nil || ch0 == '\n' {
			return buf.String(), errBadString
		} else if ch0 == '\\' {
			// If the next character is an escape then write the escaped char.
			// If it's not a valid escape then return an error.
			ch1, _, _ := r.ReadRune()
			if ch1 == 'n' {
				_, _ = buf.WriteRune('\n')
			} else if ch1 == '\\' {
				_, _ = buf.WriteRune('\\')
			} else if ch1 == '"' {
				_, _ = buf.WriteRune('"')
			} else if ch1 == '`' {
				_, _ = buf.WriteRune('`')
			} else if ch1 == '\'' {
				_, _ = buf.WriteRune('\'')
			} else if ch1 == 'x' && i == 0 {
				_, _ = buf.WriteString(`\x`)
			} else {
				return string(ch0) + string(ch1), errBadEscape
			}
		} else {
			_, _ = buf.WriteRune(ch0)
		}
	}
}

var errBadString = errors.New("bad string")
var errBadEscape = errors.New("bad escape")

// scanBareIdent reads bare identifier from a rune reader.
func scanBareIdent(r io.RuneScanner) string {
	// Read every ident character into the buffer.
	// Non-ident characters and EOF will cause the loop to exit.
	var buf bytes.Buffer
	for {
		ch, _, err := r.ReadRune()
		if err != nil {
			break
		} else if !isIdentChar(ch) {
			r.UnreadRune()
			break
		} else {
			_, _ = buf.WriteRune(ch)
		}
	}
	return buf.String()
}
