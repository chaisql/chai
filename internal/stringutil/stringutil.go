package stringutil

import (
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