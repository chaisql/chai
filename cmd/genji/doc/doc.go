/* Package doc provides an API to access documentation for functions and tokens */
package doc

import (
	"errors"
	"fmt"
	"strings"

	"github.com/genjidb/genji/internal/expr/functions"
	"github.com/genjidb/genji/internal/sql/scanner"
)

var ErrNotFound = errors.New("No documentation found")
var ErrInvalid = errors.New("Invalid documentation query")

// DocString returns a string containing the documentation for a given expression.
//
// The expression is merely scanned and not parsed, looking for keywords tokens or IDENT
// tokens forming a function. In that last case, a function lookup is performed, yielding the
// documentation of that particular function.
func DocString(rawExpr string) (string, error) {
	if rawExpr == "" {
		return "", ErrInvalid
	}
	s := scanner.NewScanner(strings.NewReader(rawExpr))
	tok, _, _ := s.Scan()
	if tok == scanner.ILLEGAL {
		return "", ErrInvalid
	}
	if tok == scanner.IDENT {
		s.Unscan()
		return scanFuncDocString(s)
	}
	docstr, ok := tokenDocs[tok]
	if ok {
		return docstr, nil
	}
	return "", ErrNotFound
}

func scanFuncDocString(s *scanner.Scanner) (string, error) {
	tok1, _, lit1 := s.Scan()
	if tok1 != scanner.IDENT {
		return "", ErrInvalid
	}
	tok2, _, _ := s.Scan()
	if tok2 != scanner.EOF && tok2 == scanner.DOT {
		// tok1 is a package because tok2 is a "."
		tok3, _, lit3 := s.Scan()
		if tok3 != scanner.IDENT {
			return "", ErrInvalid
		}
		return funcDocString(lit1, lit3)
	} else {
		// no package, it's a builtin function
		return funcDocString("", lit1)
	}
}

func funcDocString(pkg string, name string) (string, error) {
	table := functions.DefaultPackagesTable()
	f, err := table.GetFunc(pkg, name)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrNotFound, err.Error())
	}
	// Because we got the definition, we know that the package and function both exist.
	p := packageDocs[pkg]
	d := p[name]
	if pkg != "" {
		return fmt.Sprintf("%s.%s: %s", pkg, f.String(), d), nil
	} else {
		return fmt.Sprintf("%s: %s", f.String(), d), nil
	}
}
