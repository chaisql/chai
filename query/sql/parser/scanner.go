package parser

import (
	"bufio"
	"bytes"
	"io"
	"strings"
)

// Token represents a lexical token.
type Token int

// List of valid tokens
const (
	// Special tokens
	ILLEGAL Token = iota
	EOF
	WS

	// Literals
	IDENT   // fields, table_name
	NUMBER  // 12345.67
	INTEGER // 12345
	STRING  // "abc"
	TRUE    // true
	FALSE   // false

	// Keywords
	SELECT
	FROM
	WHERE

	// Operators
	EQ  // =
	GT  // >
	GTE // >=
	LT  // <
	LTE // <=

	// Misc characters
	ASTERISK // *
	COMMA    // ,
)

var eof = rune(0)

func isWhitespace(ch rune) bool {
	return ch == ' ' || ch == '\t' || ch == '\n'
}

func isLetter(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isDigit(ch rune) bool {
	return ch >= '0' && ch <= '9'
}

// Scanner represents a lexical scanner.
type Scanner struct {
	r *bufio.Reader
}

// NewScanner returns a new instance of Scanner.
func NewScanner(r io.Reader) *Scanner {
	return &Scanner{r: bufio.NewReader(r)}
}

// read reads the next rune from the bufferred reader.
// Returns the rune(0) if an error occurs (or io.EOF is returned).
func (s *Scanner) read() rune {
	ch, _, err := s.r.ReadRune()
	if err != nil {
		return eof
	}
	return ch
}

// unread places the previously read rune back on the reader.
func (s *Scanner) unread() { _ = s.r.UnreadRune() }

// Scan returns the next token and literal value.
func (s *Scanner) Scan() (tok Token, lit string) {
	// Read the next rune.
	ch := s.read()

	if isWhitespace(ch) {
		// If we see whitespace then consume all contiguous whitespace.
		s.unread()
		return s.scanWhitespace()
	} else if isDigit(ch) || ch == '-' {
		// If we see a digit or the minus sign consume a number.
		s.unread()
		return s.scanNumber()
	} else if isLetter(ch) {
		// If we see a letter then consume as an ident or reserved word.
		s.unread()
		return s.scanIdent()
	}

	// Otherwise read the individual character.
	switch ch {
	case eof:
		return EOF, ""
	case '*':
		return ASTERISK, string(ch)
	case ',':
		return COMMA, string(ch)
	}

	return ILLEGAL, string(ch)
}

// scanWhitespace consumes the current rune and all contiguous whitespace.
func (s *Scanner) scanWhitespace() (tok Token, lit string) {
	// Create a buffer and read the current character into it.
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	// Read every subsequent whitespace character into the buffer.
	// Non-whitespace characters and EOF will cause the loop to exit.
	for {
		if ch := s.read(); ch == eof {
			break
		} else if !isWhitespace(ch) {
			s.unread()
			break
		} else {
			buf.WriteRune(ch)
		}
	}

	return WS, buf.String()
}

// scanIdent consumes the current rune and all contiguous ident runes.
func (s *Scanner) scanIdent() (tok Token, lit string) {
	// Create a buffer and read the current character into it.
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	// Read every subsequent ident character into the buffer.
	// Non-ident characters and EOF will cause the loop to exit.
	for {
		if ch := s.read(); ch == eof {
			break
		} else if !isLetter(ch) && !isDigit(ch) && ch != '_' {
			s.unread()
			break
		} else {
			_, _ = buf.WriteRune(ch)
		}
	}

	// If the string matches a keyword then return that keyword.
	switch strings.ToUpper(buf.String()) {
	case "SELECT":
		return SELECT, buf.String()
	case "FROM":
		return FROM, buf.String()
	case "WHERE":
		return WHERE, buf.String()
	}

	// Otherwise return as a regular identifier.
	return IDENT, buf.String()
}

// scanNumber consumes anything that looks like the start of a number.
func (s *Scanner) scanNumber() (tok Token, lit string) {
	var buf bytes.Buffer

	// Check if the initial rune is a "-".
	ch := s.read()
	if ch == '-' {
		buf.WriteRune(ch)
	} else {
		s.unread()
	}

	// Read as many digits as possible.
	_, _ = buf.WriteString(s.scanDigits())

	// If next code points are a full stop and digit then consume them.
	isDecimal := false
	if ch0 := s.read(); ch0 == '.' {
		isDecimal = true
		if ch1 := s.read(); isDigit(ch1) {
			_, _ = buf.WriteRune(ch0)
			_, _ = buf.WriteRune(ch1)
			_, _ = buf.WriteString(s.scanDigits())
		} else {
			s.unread()
		}
	} else {
		s.unread()
	}

	// Read as an integer if it doesn't have a fractional part.
	if !isDecimal {
		return INTEGER, buf.String()
	}
	return NUMBER, buf.String()
}

// scanDigits consumes a contiguous series of digits.
func (s *Scanner) scanDigits() string {
	var buf bytes.Buffer
	for {
		ch := s.read()
		if !isDigit(ch) {
			s.unread()
			break
		}
		_, _ = buf.WriteRune(ch)
	}
	return buf.String()
}
