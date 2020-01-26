package scanner

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
)

// Code heavily inspired by the influxdata/influxql repository
// https://github.com/influxdata/influxql/blob/57f403b00b124eb900835c0c944e9b60d848db5e/scanner.go#L12

// Scanner represents a lexical scanner for Genji.
type Scanner struct {
	r *reader
}

// NewScanner returns a new instance of Scanner.
func NewScanner(r io.Reader) *Scanner {
	return &Scanner{r: &reader{r: bufio.NewReaderSize(r, 128)}}
}

// Scan returns the next token and position from the underlying reader.
// Also returns the literal text read for strings, numbers, and duration tokens
// since these token types can have different literal representations.
func (s *Scanner) Scan() TokenInfo {
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
		return TokenInfo{EOF, pos, ""}
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
		return TokenInfo{DOT, pos, ""}
	case '$':
		ti := s.scanIdent(false)
		if ti.Tok != IDENT {
			return TokenInfo{ti.Tok, pos, "$" + ti.Lit}
		}
		return TokenInfo{NAMEDPARAM, pos, "$" + ti.Lit}
	case '?':
		return TokenInfo{POSITIONALPARAM, pos, ""}
	case '+':
		return TokenInfo{ADD, pos, ""}
	case '-':
		ch1, _ := s.r.read()
		if ch1 == '-' {
			s.skipUntilNewline()
			return TokenInfo{COMMENT, pos, ""}
		}
		if isDigit(ch1) {
			s.r.unread()
			return s.scanNumber()
		}
		s.r.unread()
		return TokenInfo{SUB, pos, ""}
	case '*':
		return TokenInfo{MUL, pos, ""}
	case '/':
		ch1, _ := s.r.read()
		if ch1 == '*' {
			if err := s.skipUntilEndComment(); err != nil {
				return TokenInfo{ILLEGAL, pos, ""}
			}
			return TokenInfo{COMMENT, pos, ""}
		}
		s.r.unread()
		return TokenInfo{DIV, pos, ""}
	case '%':
		return TokenInfo{MOD, pos, ""}
	case '&':
		return TokenInfo{BITWISEAND, pos, ""}
	case '|':
		return TokenInfo{BITWISEOR, pos, ""}
	case '^':
		return TokenInfo{BITWISEXOR, pos, ""}
	case '=':
		ch1, _ := s.r.read()
		if ch1 == '~' {
			return TokenInfo{EQREGEX, pos, ""}
		}
		if ch1 == '=' {
			return TokenInfo{EQ, pos, ""}
		}
		s.r.unread()
		return TokenInfo{EQ, pos, ""}
	case '!':
		if ch1, _ := s.r.read(); ch1 == '=' {
			return TokenInfo{NEQ, pos, ""}
		} else if ch1 == '~' {
			return TokenInfo{NEQREGEX, pos, ""}
		}
		s.r.unread()
	case '>':
		if ch1, _ := s.r.read(); ch1 == '=' {
			return TokenInfo{GTE, pos, ""}
		}
		s.r.unread()
		return TokenInfo{GT, pos, ""}
	case '<':
		if ch1, _ := s.r.read(); ch1 == '=' {
			return TokenInfo{LTE, pos, ""}
		} else if ch1 == '>' {
			return TokenInfo{NEQ, pos, ""}
		}
		s.r.unread()
		return TokenInfo{LT, pos, ""}
	case '(':
		return TokenInfo{LPAREN, pos, ""}
	case ')':
		return TokenInfo{RPAREN, pos, ""}
	case '{':
		return TokenInfo{LBRACKET, pos, ""}
	case '}':
		return TokenInfo{RBRACKET, pos, ""}
	case '[':
		return TokenInfo{LSBRACKET, pos, ""}
	case ']':
		return TokenInfo{RSBRACKET, pos, ""}
	case ',':
		return TokenInfo{COMMA, pos, ""}
	case ';':
		return TokenInfo{SEMICOLON, pos, ""}
	case ':':
		if ch1, _ := s.r.read(); ch1 == ':' {
			return TokenInfo{DOUBLECOLON, pos, ""}
		}
		s.r.unread()
		return TokenInfo{COLON, pos, ""}
	}

	return TokenInfo{ILLEGAL, pos, string(ch0)}
}

// scanWhitespace consumes the current rune and all contiguous whitespace.
func (s *Scanner) scanWhitespace() TokenInfo {
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

	return TokenInfo{WS, pos, buf.String()}
}

// skipUntilNewline skips characters until it reaches a newline.
func (s *Scanner) skipUntilNewline() {
	for {
		if ch, _ := s.r.read(); ch == '\n' || ch == eof {
			return
		}
	}
}

// skipUntilEndComment skips characters until it reaches a '*/' symbol.
func (s *Scanner) skipUntilEndComment() error {
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

func (s *Scanner) scanIdent(lookup bool) TokenInfo {
	// Save the starting position of the identifier.
	_, pos := s.r.read()
	s.r.unread()

	var buf bytes.Buffer
	for {
		if ch, _ := s.r.read(); ch == eof {
			break
		} else if ch == '`' {
			ti0 := s.scanString()
			if ti0.Tok == BADSTRING || ti0.Tok == BADESCAPE {
				return ti0
			}
			return TokenInfo{IDENT, pos, ti0.Lit}
		} else if isIdentChar(ch) {
			s.r.unread()
			buf.WriteString(ScanBareIdent(s.r))
		} else {
			s.r.unread()
			break
		}
	}
	lit := buf.String()

	// If the literal matches a keyword then return that keyword.
	if lookup {
		if tok := Lookup(lit); tok != IDENT {
			return TokenInfo{tok, pos, ""}
		}
	}
	return TokenInfo{IDENT, pos, lit}
}

// scanString consumes a contiguous string of non-quote characters.
// Quote characters can be consumed if they're first escaped with a backslash.
func (s *Scanner) scanString() TokenInfo {
	s.r.unread()
	_, pos := s.r.curr()

	lit, err := ScanString(s.r)
	if err == errBadString {
		return TokenInfo{BADSTRING, pos, lit}
	} else if err == errBadEscape {
		_, pos = s.r.curr()
		return TokenInfo{BADESCAPE, pos, lit}
	}
	return TokenInfo{STRING, pos, lit}
}

// ScanRegex consumes a token to find escapes
func (s *Scanner) ScanRegex() TokenInfo {
	_, pos := s.r.curr()

	// Start & end sentinels.
	start, end := '/', '/'
	// Valid escape chars.
	escapes := map[rune]rune{'/': '/'}

	b, err := ScanDelimited(s.r, start, end, escapes, true)

	if err == errBadEscape {
		_, pos = s.r.curr()
		return TokenInfo{BADESCAPE, pos, ""}
	} else if err != nil {
		return TokenInfo{BADREGEX, pos, ""}
	}
	return TokenInfo{REGEX, pos, string(b)}
}

// scanNumber consumes anything that looks like the start of a number.
func (s *Scanner) scanNumber() TokenInfo {
	var buf bytes.Buffer

	// Check if the initial rune is a ".".
	ch, pos := s.r.curr()
	if ch == '.' {
		// Peek and see if the next rune is a digit.
		ch1, _ := s.r.read()
		s.r.unread()
		if !isDigit(ch1) {
			return TokenInfo{ILLEGAL, pos, "."}
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

	// Read as a duration or integer if it doesn't have a fractional part.
	if !isDecimal {
		// If the next rune is a letter then this is a duration token.
		if ch0, _ := s.r.read(); isLetter(ch0) || ch0 == 'µ' {
			_, _ = buf.WriteRune(ch0)
			for {
				ch1, _ := s.r.read()
				if !isLetter(ch1) && ch1 != 'µ' {
					s.r.unread()
					break
				}
				_, _ = buf.WriteRune(ch1)
			}

			// Continue reading digits and letters as part of this token.
			for {
				if ch0, _ := s.r.read(); isLetter(ch0) || ch0 == 'µ' || isDigit(ch0) {
					_, _ = buf.WriteRune(ch0)
				} else {
					s.r.unread()
					break
				}
			}
			return TokenInfo{DURATIONVAL, pos, buf.String()}
		}
		s.r.unread()
		return TokenInfo{INTEGER, pos, buf.String()}
	}
	return TokenInfo{NUMBER, pos, buf.String()}
}

// scanDigits consumes a contiguous series of digits.
func (s *Scanner) scanDigits() string {
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

// isIdentFirstChar returns true if the rune can be used as the first char in an unquoted identifer.
func isIdentFirstChar(ch rune) bool { return isLetter(ch) || ch == '_' }

// BufScanner represents a wrapper for scanner to add a buffer.
// It provides a fixed-length circular buffer that can be unread.
type BufScanner struct {
	s   *Scanner
	i   int // buffer index
	n   int // buffer size
	buf [3]TokenInfo
}

// NewBufScanner returns a new buffered scanner for a reader.
func NewBufScanner(r io.Reader) *BufScanner {
	return &BufScanner{s: NewScanner(r)}
}

// Scan reads the next token from the scanner.
func (s *BufScanner) Scan() TokenInfo {
	return s.scanFunc(s.s.Scan)
}

// ScanRegex reads a regex token from the scanner.
func (s *BufScanner) ScanRegex() TokenInfo {
	return s.scanFunc(s.s.ScanRegex)
}

// scanFunc uses the provided function to scan the next token.
func (s *BufScanner) scanFunc(scan func() TokenInfo) TokenInfo {
	// If we have unread tokens then read them off the buffer first.
	if s.n > 0 {
		s.n--
		return s.curr()
	}

	// Move buffer position forward and save the token.
	s.i = (s.i + 1) % len(s.buf)
	s.buf[s.i] = scan()

	return s.curr()
}

// Unscan pushes the previously token back onto the buffer.
func (s *BufScanner) Unscan() { s.n++ }

// curr returns the last read token.
func (s *BufScanner) curr() TokenInfo {
	return s.buf[(s.i-s.n+len(s.buf))%len(s.buf)]
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

// ScanDelimited reads a delimited set of runes
func ScanDelimited(r io.RuneScanner, start, end rune, escapes map[rune]rune, escapesPassThru bool) ([]byte, error) {
	// Scan start delimiter.
	if ch, _, err := r.ReadRune(); err != nil {
		return nil, err
	} else if ch != start {
		return nil, fmt.Errorf("expected %s; found %s", string(start), string(ch))
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

// ScanString reads a quoted string from a rune reader.
func ScanString(r io.RuneScanner) (string, error) {
	ending, _, err := r.ReadRune()
	if err != nil {
		return "", errBadString
	}

	var buf bytes.Buffer
	for {
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

// ScanBareIdent reads bare identifier from a rune reader.
func ScanBareIdent(r io.RuneScanner) string {
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

// IsRegexOp returns true if the operator accepts a regex operand.
func IsRegexOp(t Token) bool {
	return (t == EQREGEX || t == NEQREGEX)
}

// TokenInfo holds information about a token.
type TokenInfo struct {
	Tok Token
	Pos Pos
	Lit string
}
