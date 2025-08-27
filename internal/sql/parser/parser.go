package parser

import (
	"fmt"
	"io"
	"strings"

	"github.com/chaisql/chai/internal/expr"
	"github.com/chaisql/chai/internal/query"
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/scanner"
	"github.com/chaisql/chai/internal/tree"
	"github.com/cockroachdb/errors"
)

// Parser represents an Chai SQL Parser.
type Parser struct {
	s *scanner.Scanner
}

// NewParser returns a new instance of Parser.
func NewParser(r io.Reader) *Parser {
	return &Parser{s: scanner.NewScanner(r)}
}

// ParseQuery parses a query string and returns its AST representation.
func ParseQuery(s string) (query.Query, error) {
	return NewParser(strings.NewReader(s)).ParseQuery()
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

// ParseQuery parses a Chai SQL string and returns a Query.
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

// ParseQuery parses a Chai SQL string and returns a Query.
func (p *Parser) Parse(fn func(statement.Statement) error) error {
	for {
		err := p.skipMany(scanner.SEMICOLON)
		if err != nil {
			return err
		}

		end, err := p.parseOptional(scanner.EOF)
		if err != nil {
			return err
		}
		if end {
			return nil
		}

		s, err := p.ParseStatement()
		if err != nil {
			return err
		}

		tok, pos, lit := p.ScanIgnoreWhitespace()
		switch tok {
		case scanner.EOF:
			return fn(s)
		case scanner.SEMICOLON:
			err = fn(s)
			if err != nil {
				return err
			}
		default:
			p.Unscan()
			return newParseError(scanner.Tokstr(tok, lit), []string{";"}, pos)
		}
	}
}

// ParseStatement parses a Chai SQL string and returns a statement.
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

func (p *Parser) skipMany(tok scanner.Token) error {
	for {
		t, _, _ := p.ScanIgnoreWhitespace()
		if t != tok {
			p.Unscan()
			return nil
		}
	}
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

// parseColumnList parses a list of columns in the form: (path, path, ...), if exists
func (p *Parser) parseColumnList() ([]string, tree.SortOrder, error) {
	// Parse ( token.
	if ok, err := p.parseOptional(scanner.LPAREN); !ok || err != nil {
		return nil, 0, err
	}

	var columns []string
	var err error
	var col string
	var order tree.SortOrder

	// Parse first (required) column.
	if col, err = p.parseIdent(); err != nil {
		return nil, 0, err
	}

	columns = append(columns, col)

	// Parse optional ASC/DESC token.
	ok, err := p.parseOptional(scanner.DESC)
	if err != nil {
		return nil, 0, err
	}
	if ok {
		order = order.SetDesc(0)
	} else {
		// ignore ASC if set
		_, err := p.parseOptional(scanner.ASC)
		if err != nil {
			return nil, 0, err
		}
	}

	// Parse remaining (optional) columns.
	i := 0
	for {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			break
		}

		c, err := p.parseIdent()
		if err != nil {
			return nil, 0, err
		}

		columns = append(columns, c)

		i++

		// Parse optional ASC/DESC token.
		ok, err := p.parseOptional(scanner.DESC)
		if err != nil {
			return nil, 0, err
		}
		if ok {
			order = order.SetDesc(i)
		} else {
			// ignore ASC if set
			_, err := p.parseOptional(scanner.ASC)
			if err != nil {
				return nil, 0, err
			}
		}
	}

	// Parse required ) token.
	if err := p.ParseTokens(scanner.RPAREN); err != nil {
		return nil, 0, err
	}

	return columns, order, nil
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

// ParseTokens parses all the given tokens one after the other.
// It returns an error if one of the token is missing.
func (p *Parser) ParseTokens(tokens ...scanner.Token) error {
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

	err := p.ParseTokens(tokens[1:]...)
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
