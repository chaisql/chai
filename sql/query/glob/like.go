// The author disclaims copyright to this source code.  In place of
// a legal notice, here is a blessing:
//
//    May you do good and not evil.
//    May you find forgiveness for yourself and forgive others.
//    May you share freely, never taking more than you give.
//
// https://sqlite.org/src/file?name=ext%2Ficu%2Ficu.c&ln=117-195&m=54b54f02c66c5aea

package glob

import (
	"unicode"
	"unicode/utf8"
)

const (
	matchOne = '_'
	matchAll = '%'
	matchEsc = '\\'
)

// readRune is like skipRune, but also returns the removed Unicode code point.
func readRune(s string) (rune, string) {
	r, size := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError && size == 1 {
		return rune(s[0]), s[1:]
	}
	return r, s[size:]
}

// skipRune returns a slice of the string s with the first Unicode code point removed.
func skipRune(s string) string {
	_, size := utf8.DecodeRuneInString(s)
	return s[size:]
}

// equalFold is strings.EqualFold for individual runes.
func equalFold(sr, tr rune) bool {
	// Easy case.
	if tr == sr {
		return true
	}

	// Make sr < tr to simplify what follows.
	if tr < sr {
		tr, sr = sr, tr
	}
	// Fast check for ASCII.
	if tr < utf8.RuneSelf {
		// ASCII only, sr/tr must be upper/lower case
		return 'A' <= sr && sr <= 'Z' && tr == sr+'a'-'A'
	}

	// General case. SimpleFold(x) returns the next equivalent rune > x
	// or wraps around to smaller values.
	r := unicode.SimpleFold(sr)
	for r != sr && r < tr {
		r = unicode.SimpleFold(r)
	}
	return r == tr
}

// MatchLike reports whether string s matches the SQL LIKE-style glob pattern.
// Supported wildcards are '_' (match any one character) and '%' (match zero
// or more characters). They can be escaped by '\' (escape character).
//
// MatchLike requires pattern to match whole string, not just a substring.
func MatchLike(pattern, s string) bool {
	var prevEscape bool

	var w, t string // backtracking state

	for len(s) != 0 {
		if len(pattern) == 0 {
			return false
		}

		// Read (and consume) the next character from the input pattern.
		var p rune
		p, pattern = readRune(pattern)

	backtrack:
		// There are now 4 possibilities:
		//
		// 1. p is an unescaped match-all character "%",
		// 2. p is an unescaped match-one character "_",
		// 3. p is an unescaped escape character, or
		// 4. p is to be handled as an ordinary character
		//
		if p == matchAll && !prevEscape {
			// Case 1.
			var c byte

			// Skip any matchAll or matchOne characters that follow a
			// matchAll. For each matchOne, skip one character in the
			// test string.
			//
			for len(pattern) != 0 {
				c = pattern[0]
				if c != matchAll && c != matchOne {
					break
				}
				pattern = pattern[1:]

				if c != matchOne {
					continue
				}
				if len(s) == 0 {
					return false
				}
				s = skipRune(s)
			}

			if len(pattern) == 0 {
				return true
			}

			// Save state and match next character.
			//
			w, t = pattern, s
		} else if p == matchOne && !prevEscape {
			// Case 2.
			if len(s) == 0 {
				return false
			}
			s = skipRune(s)
		} else if p == matchEsc && !prevEscape {
			// Case 3.
			prevEscape = true
		} else {
			// Case 4.
			prevEscape = false

			var r rune
			r, s = readRune(s)
			if !equalFold(p, r) {
				if len(w) == 0 {
					// Nothing to backtrack.
					return false
				}
				// Keep the pattern and skip rune in input.
				// Note that we only backtrack to matchAll.
				p, pattern = matchAll, w
				s = skipRune(t)
				goto backtrack
			}
		}
	}

	// Check that the rest of the pattern is matchAll.
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == matchAll {
			continue
		}

		// Allow escaping end of string.
		if i+1 == len(pattern) {
			if pattern[i] == matchEsc {
				return true
			}
		}

		return false
	}
	return true
}
