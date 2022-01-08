package stringutil

import (
	"strings"
	"unicode/utf8"
)

// NeedsQuotes reports whether s should be wrapped with quote
// before being used as a document key.
// If it returns true, s must be used with strconv.Quote or
// using the fmt '%q' formatter.
func NeedsQuotes(s string) bool {
	if s == "" {
		return true
	}

	for len(s) > 0 {
		r, wid := utf8.DecodeRuneInString(s)
		if wid > 1 {
			return true
		}

		if r == utf8.RuneError {
			return true
		}

		if ('0' <= r && r <= '9') || ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z') || r == '_' {
			s = s[wid:]
			continue
		}

		return true
	}

	return false
}

// NormalizeIdentifier wraps s around the given quotes, if needed.
func NormalizeIdentifier(s string, with rune) string {
	if s == "" {
		return s
	}

	if !NeedsQuotes(s) {
		return s
	}

	var sb strings.Builder

	sb.WriteRune(with)

	for len(s) > 0 {
		r, wid := utf8.DecodeRuneInString(s)

		if r == with {
			sb.WriteRune('\\')
		}
		sb.WriteRune(r)
		s = s[wid:]
	}

	sb.WriteRune(with)

	return sb.String()
}

func Contains(slice []string, s string) bool {
	for _, ss := range slice {
		if ss == s {
			return true
		}
	}

	return false
}
