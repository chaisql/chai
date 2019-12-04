package parser

import (
	"fmt"
	"io"
	"strings"

	"github.com/asdine/genji/query"
	"github.com/asdine/genji/scanner"
)

// Parser represents an Genji SQL Parser.
type Parser struct {
	s             *scanner.BufScanner
	orderedParams int
	namedParams   int
	stat          parserStat
}

// NewParser returns a new instance of Parser.
func NewParser(r io.Reader) *Parser {
	return &Parser{s: scanner.NewBufScanner(r)}
}

// ParseQuery parses a query string and returns its AST representation.
func ParseQuery(s string) (query.Query, error) { return NewParser(strings.NewReader(s)).ParseQuery() }

// ParseQuery parses a Genji SQL string and returns a Query.
func (p *Parser) ParseQuery() (query.Query, error) {
	var statements []query.Statement
	semi := true

	for {
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok == scanner.EOF {
			return query.New(statements...), nil
		} else if tok == scanner.SEMICOLON {
			semi = true
		} else {
			if !semi {
				return query.Query{}, newParseError(scanner.Tokstr(tok, lit), []string{";"}, pos)
			}
			p.Unscan()
			s, err := p.ParseStatement()
			if err != nil {
				return query.Query{}, err
			}
			statements = append(statements, s)
			semi = false
		}
	}
}

// ParseStatement parses a Genji SQL string and returns a Statement AST object.
func (p *Parser) ParseStatement() (query.Statement, error) {
	tok, pos, lit := p.ScanIgnoreWhitespace()
	switch tok {
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
	}

	return nil, newParseError(scanner.Tokstr(tok, lit), []string{
		"SELECT", "DELETE", "UPDATE", "INSERT", "CREATE", "DROP",
	}, pos)
}

// parseCondition parses the "WHERE" clause of the query, if it exists.
func (p *Parser) parseCondition() (query.Expr, error) {
	// Check if the WHERE token exists.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.WHERE {
		p.Unscan()
		return nil, nil
	}

	// Scan the identifier for the source.
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}

	return expr, nil
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
func (p *Parser) Unscan() { p.s.Unscan() }

// parserStat carries contextual information
// discovered while parsing queries.
type parserStat struct {
	exprFields []string
}

// ParseError represents an error that occurred during parsing.
type ParseError struct {
	Message  string
	Found    string
	Expected []string
	Pos      scanner.Pos
}

// newParseError returns a new instance of ParseError.
func newParseError(found string, expected []string, pos scanner.Pos) *ParseError {
	return &ParseError{Found: found, Expected: expected, Pos: pos}
}

// Error returns the string representation of the error.
func (e *ParseError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("%s at line %d, char %d", e.Message, e.Pos.Line+1, e.Pos.Char+1)
	}
	return fmt.Sprintf("found %s, expected %s at line %d, char %d", e.Found, strings.Join(e.Expected, ", "), e.Pos.Line+1, e.Pos.Char+1)
}
