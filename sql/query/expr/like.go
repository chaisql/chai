package expr

import (
	"errors"
	"fmt"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/scanner"
	"regexp"
	"strings"
)

func isWildcard(char byte) bool {
	return char == '%' || char == '_'
}

// replaceUnescaped replaces all instances of oldStr that are not escaped (read:
// preceded) with the specified unescape token with newStr.
// For example, with an escape token of `\\`
//    replaceUnescaped("TE\\__ST", "_", ".", `\\`) --> "TE\\_.ST"
//    replaceUnescaped("TE\\%%ST", "%", ".*", `\\`) --> "TE\\%.*ST"
// If the preceding escape token is escaped, then oldStr will be replaced.
// For example
//    replaceUnescaped("TE\\\\_ST", "_", ".", `\\`) --> "TE\\\\.ST"
func replaceUnescaped(s, oldStr, newStr string, escapeToken string) string {
	// We count the number of occurrences of 'oldStr'.
	// This however can be an overestimate since the oldStr token could be
	// escaped.  e.g. `\\_`.
	nOld := strings.Count(s, oldStr)
	if nOld == 0 {
		return s
	}

	// Allocate buffer for final string.
	// This can be an overestimate since some of the oldStr tokens may
	// be escaped.
	// This is fine since we keep track of the running number of bytes
	// actually copied.
	// It's rather difficult to count the exact number of unescaped
	// tokens without manually iterating through the entire string and
	// keeping track of escaped escape tokens.
	retLen := len(s)
	// If len(newStr) - len(oldStr) < 0, then this can under-allocate which
	// will not behave correctly with copy.
	if addnBytes := nOld * (len(newStr) - len(oldStr)); addnBytes > 0 {
		retLen += addnBytes
	}
	ret := make([]byte, retLen)
	retWidth := 0
	start := 0
OldLoop:
	for i := 0; i < nOld; i++ {
		nextIdx := start + strings.Index(s[start:], oldStr)

		escaped := false
		for {
			// We need to look behind to check if the escape token
			// is really an escape token.
			// E.g. if our specified escape token is `\\` and oldStr
			// is `_`, then
			//    `\\_` --> escaped
			//    `\\\\_` --> not escaped
			//    `\\\\\\_` --> escaped
			curIdx := nextIdx
			lookbehindIdx := curIdx - len(escapeToken)
			for lookbehindIdx >= 0 && s[lookbehindIdx:curIdx] == escapeToken {
				escaped = !escaped
				curIdx = lookbehindIdx
				lookbehindIdx = curIdx - len(escapeToken)
			}

			// The token was not be escaped. Proceed.
			if !escaped {
				break
			}

			// Token was escaped. Copy everything over and continue.
			retWidth += copy(ret[retWidth:], s[start:nextIdx+len(oldStr)])
			start = nextIdx + len(oldStr)

			// Continue with next oldStr token.
			continue OldLoop
		}

		// Token was not escaped so we replace it with newStr.
		// Two copies is more efficient than concatenating the slices.
		retWidth += copy(ret[retWidth:], s[start:nextIdx])
		retWidth += copy(ret[retWidth:], newStr)
		start = nextIdx + len(oldStr)
	}

	retWidth += copy(ret[retWidth:], s[start:])
	return string(ret[0:retWidth])
}

func patternToRegexp(pattern string) (*regexp.Regexp, error) {
	pattern = regexp.QuoteMeta(pattern)
	pattern = replaceUnescaped(pattern, `%`, `.*`, `\\`)
	pattern = replaceUnescaped(pattern, `_`, `.`, `\\`)
	return regexp.Compile(pattern)
}

func like(text, pattern string) (bool, error) {
	if pattern == "" {
		// true only if text == pattern => text == "" => len(text) == 0
		return len(text) == 0, nil
	}

	if pattern == "%" {
		// any match
		return true, nil
	}

	if pattern == "_" {
		// one any character or more
		return len(text) > 0, nil
	}

	if len(pattern) > 1 && !strings.ContainsAny(pattern[1:len(pattern)-1], "%_") {
		first := pattern[0]
		last := pattern[len(pattern)-1]
		switch {
		case !isWildcard(first) && !isWildcard(last):
			// exact match
			return text == pattern, nil
		case first == '%' && !isWildcard(last):
			// suffix match
			return strings.HasSuffix(text, pattern[1:]), nil
		case last == '%' && !isWildcard(first):
			// prefix match
			return strings.HasPrefix(text, pattern[0:len(pattern)-1]), nil
		case first == '%' && last == '%':
			// contains
			return strings.Contains(text, pattern[1:len(pattern)-1]), nil
		}
	}

	r, err := patternToRegexp(pattern)
	if err != nil {
		return false, err
	}
	return r.MatchString(text), nil
}

type likeOp struct {
	*simpleOperator
}

// Like creates an expression that evaluates to the result of a LIKE b.
func Like(a, b Expr) Expr {
	return &likeOp{&simpleOperator{a, b, scanner.LIKE}}
}

func (op likeOp) Eval(ctx EvalStack) (document.Value, error) {
	a, b, err := op.simpleOperator.eval(ctx)
	if err != nil {
		return nullLitteral, err
	}

	if a.Type != document.TextValue || b.Type != document.TextValue {
		return nullLitteral, errors.New("LIKE operator takes an text")
	}

	ok, err := like(a.V.(string), b.V.(string))
	if err != nil {
		return nullLitteral, err
	}
	if ok {
		return trueLitteral, nil
	}

	return falseLitteral, nil
}

func (op likeOp) String() string {
	return fmt.Sprintf("%v LIKE %v", op.a, op.b)
}

type notLikeOp struct {
	likeOp
}

// NotLike creates an expression that evaluates to the result of a NOT LIKE b.
func NotLike(a, b Expr) Expr {
	return &notLikeOp{likeOp{&simpleOperator{a, b, scanner.LIKE}}}
}

func (op notLikeOp) Eval(ctx EvalStack) (document.Value, error) {
	return invertBoolResult(op.likeOp.Eval)(ctx)
}

func (op notLikeOp) String() string {
	return fmt.Sprintf("%v NOT LIKE %v", op.a, op.b)
}
