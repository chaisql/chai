package parser

import (
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/expr/functions"
	"github.com/genjidb/genji/internal/query"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/scanner"
)

// Parser represents an Genji SQL Parser.
type Parser struct {
	s             *scanner.Scanner
	orderedParams int
	namedParams   int
	packagesTable functions.Packages
}

// NewParser returns a new instance of Parser.
func NewParser(r io.Reader) *Parser {
	return NewParserWithOptions(r, nil)
}

// NewParserWithOptions returns a new instance of Parser using given Options.
func NewParserWithOptions(r io.Reader, opts *Options) *Parser {
	if opts == nil {
		opts = defaultOptions()
	}

	return &Parser{s: scanner.NewScanner(r), packagesTable: opts.Packages}
}

// ParseQuery parses a query string and returns its AST representation.
func ParseQuery(s string) (query.Query, error) {
	return NewParser(strings.NewReader(s)).ParseQuery()
}

// ParsePath parses a path to a value in a document.
func ParsePath(s string) (document.Path, error) {
	return NewParser(strings.NewReader(s)).parsePath()
}

// ParseExpr parses an expression.
func ParseExpr(s string) (expr.Expr, error) {
	e, err := NewParser(strings.NewReader(s)).ParseExpr()
	return e, err
}

// MustParseExpr calls ParseExpr and panics if it returns an error.
func MustParseExpr(s string) expr.Expr {
	e, err := ParseExpr(s)
	if err != nil {
		panic(fmt.Sprintf("%+v", err))
	}

	return e
}

// ParseQuery parses a Genji SQL string and returns a Query.
func (p *Parser) ParseQuery() (query.Query, error) {
	var statements []statement.Statement

	err := p.Parse(func(s statement.Statement) error {
		statements = append(statements, s)
		return nil
	})
	if err != nil {
		return query.Query{}, err
	}

	return query.Query{Statements: statements}, nil
}

// ParseQuery parses a Genji SQL string and returns a Query.
func (p *Parser) Parse(fn func(statement.Statement) error) error {
	semi := true

	for {
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok == scanner.EOF {
			return nil
		} else if tok == scanner.SEMICOLON {
			semi = true
		} else {
			if !semi {
				return newParseError(scanner.Tokstr(tok, lit), []string{";"}, pos)
			}
			p.Unscan()
			s, err := p.ParseStatement()
			if err != nil {
				return err
			}
			err = fn(s)
			if err != nil {
				return err
			}

			semi = false
		}
	}
}

// ParseStatement parses a Genji SQL string and returns a statement.
func (p *Parser) ParseStatement() (statement.Statement, error) {
	tok, pos, lit := p.ScanIgnoreWhitespace()
	p.Unscan()
	switch tok {
	case scanner.ALTER:
		return p.parseAlterStatement()
	case scanner.BEGIN:
		return p.parseBeginStatement()
	case scanner.COMMIT:
		return p.parseCommitStatement()
	case scanner.SELECT:
		return p.parseSelectStatement()
	case scanner.DELETE:
		return p.parseDeleteStatement()
	case scanner.UPDATE:
		return p.parseUpdateStatement()
	case scanner.INSERT:
		return p.parseInsertStatement()
	case scanner.CREATE:
		return p.parseCreateStatement()
	case scanner.DROP:
		return p.parseDropStatement()
	case scanner.EXPLAIN:
		return p.parseExplainStatement()
	case scanner.REINDEX:
		return p.parseReIndexStatement()
	case scanner.ROLLBACK:
		return p.parseRollbackStatement()
	}

	return nil, newParseError(scanner.Tokstr(tok, lit), []string{
		"ALTER", "BEGIN", "COMMIT", "SELECT", "DELETE", "UPDATE", "INSERT", "CREATE", "DROP", "EXPLAIN", "REINDEX", "ROLLBACK",
	}, pos)
}

// parseCondition parses the "WHERE" clause of the query, if it exists.
func (p *Parser) parseCondition() (expr.Expr, error) {
	// Check if the WHERE token exists.
	if ok, err := p.parseOptional(scanner.WHERE); !ok || err != nil {
		return nil, err
	}

	// Scan the identifier for the source.
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}

	return expr, nil
}

// parsePathList parses a list of paths in the form: (path, path, ...), if exists
func (p *Parser) parsePathList() ([]document.Path, error) {
	// Parse ( token.
	if ok, err := p.parseOptional(scanner.LPAREN); !ok || err != nil {
		return nil, err
	}

	var paths []document.Path
	var err error
	var path document.Path
	// Parse first (required) path.
	if path, err = p.parsePath(); err != nil {
		return nil, err
	}

	paths = append(paths, path)

	// Parse remaining (optional) paths.
	for {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			break
		}

		vp, err := p.parsePath()
		if err != nil {
			return nil, err
		}

		paths = append(paths, vp)
	}

	// Parse required ) token.
	if err := p.parseTokens(scanner.RPAREN); err != nil {
		return nil, err
	}

	return paths, nil
}

// Scan returns the next token from the underlying scanner.
func (p *Parser) Scan() (tok scanner.Token, pos scanner.Pos, lit string) { return p.s.Scan() }

// ScanIgnoreWhitespace scans the next non-whitespace and non-comment token.
func (p *Parser) ScanIgnoreWhitespace() (tok scanner.Token, pos scanner.Pos, lit string) {
	for {
		tok, pos, lit = p.Scan()
		if tok == scanner.WS || tok == scanner.COMMENT {
			continue
		}
		return
	}
}

// Unscan pushes the previously read token back onto the buffer.
func (p *Parser) Unscan() {
	p.s.Unscan()
}

// parseTokens parses all the given tokens one after the other.
// It returns an error if one of the token is missing.
func (p *Parser) parseTokens(tokens ...scanner.Token) error {
	for _, t := range tokens {
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != t {
			return newParseError(scanner.Tokstr(tok, lit), []string{t.String()}, pos)
		}
	}

	return nil
}

// parseOptional parses a list of consecutive tokens. If the first token is not
// present, it unscans and return false. If the first is present, all the others
// must be parsed otherwise an error is returned.
func (p *Parser) parseOptional(tokens ...scanner.Token) (bool, error) {
	// Parse optional first token
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != tokens[0] {
		p.Unscan()
		return false, nil
	}

	if len(tokens) == 1 {
		return true, nil
	}

	err := p.parseTokens(tokens[1:]...)
	return err == nil, err
}

// ParseError represents an error that occurred during parsing.
type ParseError struct {
	Message  string
	Found    string
	Expected []string
	Pos      scanner.Pos
}

// newParseError returns a new instance of ParseError.
func newParseError(found string, expected []string, pos scanner.Pos) error {
	return errors.WithStack(&ParseError{Found: found, Expected: expected, Pos: pos})
}

// Error returns the string representation of the error.
func (e *ParseError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("%s at line %d, char %d", e.Message, e.Pos.Line+1, e.Pos.Char+1)
	}
	return fmt.Sprintf("found %s, expected %s at line %d, char %d", e.Found, strings.Join(e.Expected, ", "), e.Pos.Line+1, e.Pos.Char+1)
}
