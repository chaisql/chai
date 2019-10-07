package genji

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/asdine/genji/scanner"
)

// Parser represents an Genji SQL parser.
type Parser struct {
	s             *scanner.BufScanner
	orderedParams int
	namedParams   int
}

// NewParser returns a new instance of Parser.
func NewParser(r io.Reader) *Parser {
	return &Parser{s: scanner.NewBufScanner(r)}
}

// ParseQuery parses a query string and returns its AST representation.
func ParseQuery(s string) (Query, error) { return NewParser(strings.NewReader(s)).ParseQuery() }

// ParseStatement parses a single statement and returns its AST representation.
func ParseStatement(s string) (Statement, error) {
	return NewParser(strings.NewReader(s)).ParseStatement()
}

// ParseQuery parses a Genji SQL string and returns a Query.
func (p *Parser) ParseQuery() (Query, error) {
	var statements []Statement
	semi := true

	for {
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok == scanner.EOF {
			return NewQuery(statements...), nil
		} else if tok == scanner.SEMICOLON {
			semi = true
		} else {
			if !semi {
				return Query{}, newParseError(scanner.Tokstr(tok, lit), []string{";"}, pos)
			}
			p.Unscan()
			s, err := p.ParseStatement()
			if err != nil {
				return Query{}, err
			}
			statements = append(statements, s)
			semi = false
		}
	}
}

// ParseStatement parses a Genji SQL string and returns a Statement AST object.
func (p *Parser) ParseStatement() (Statement, error) {
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
func (p *Parser) parseCondition() (Expr, error) {
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

type operator interface {
	Precedence() int
	LeftHand() Expr
	RightHand() Expr
	SetLeftHandExpr(Expr)
	SetRightHandExpr(Expr)
}

// ParseExpr parses an expression.
func (p *Parser) ParseExpr() (Expr, error) {
	var err error
	// Dummy root node.
	var root operator = &CmpOp{}

	// Parse a non-binary expression type to start.
	// This variable will always be the root of the expression tree.
	e, err := p.parseUnaryExpr()
	if err != nil {
		return nil, err
	}
	root.SetRightHandExpr(e)

	// Loop over operations and unary exprs and build a tree based on precendence.
	for {
		// If the next token is NOT an operator then return the expression.
		op, _, _ := p.ScanIgnoreWhitespace()
		if !op.IsOperator() {
			p.Unscan()
			return root.RightHand(), nil
		}

		var rhs Expr

		if rhs, err = p.parseUnaryExpr(); err != nil {
			return nil, err
		}

		// Find the right spot in the tree to add the new expression by
		// descending the RHS of the expression tree until we reach the last
		// BinaryExpr or a BinaryExpr whose RHS has an operator with
		// precedence >= the operator being added.
		for node := root.(operator); ; {
			p, ok := node.RightHand().(operator)
			if !ok || p.Precedence() >= op.Precedence() {
				// Add the new expression here and break.
				node.SetRightHandExpr(opToExpr(op, node.RightHand(), rhs))
				break
			}
			node = p
		}
	}
}

func opToExpr(op scanner.Token, lhs, rhs Expr) Expr {
	switch op {
	case scanner.EQ:
		return Eq(lhs, rhs)
	case scanner.GT:
		return Gt(lhs, rhs)
	case scanner.GTE:
		return Gte(lhs, rhs)
	case scanner.LT:
		return Lt(lhs, rhs)
	case scanner.LTE:
		return Lte(lhs, rhs)
	case scanner.AND:
		return And(lhs, rhs)
	case scanner.OR:
		return Or(lhs, rhs)
	}

	return nil
}

// parseUnaryExpr parses an non-binary expression.
func (p *Parser) parseUnaryExpr() (Expr, error) {
	tok, pos, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.IDENT:
		return FieldSelector(lit), nil
	case scanner.NAMEDPARAM:
		if len(lit) == 1 {
			return nil, &ParseError{Message: "missing param name"}
		}
		if p.orderedParams > 0 {
			return nil, &ParseError{Message: "can't mix positional arguments with named arguments"}
		}
		p.namedParams++
		return NamedParam(lit[1:]), nil
	case scanner.POSITIONALPARAM:
		if p.namedParams > 0 {
			return nil, &ParseError{Message: "can't mix positional arguments with named arguments"}
		}
		p.orderedParams++
		return PositionalParam(p.orderedParams), nil
	case scanner.STRING:
		return StringValue(lit), nil
	case scanner.NUMBER:
		v, err := strconv.ParseFloat(lit, 64)
		if err != nil {
			return nil, &ParseError{Message: "unable to parse number", Pos: pos}
		}
		return Float64Value(v), nil
	case scanner.INTEGER:
		v, err := strconv.ParseInt(lit, 10, 64)
		if err != nil {
			// The literal may be too large to fit into an int64. If it is, use an unsigned integer.
			// The check for negative numbers is handled somewhere else so this should always be a positive number.
			if v, err := strconv.ParseUint(lit, 10, 64); err == nil {
				return Uint64Value(v), nil
			}
			return nil, &ParseError{Message: "unable to parse integer", Pos: pos}
		}
		return Int64Value(v), nil
	case scanner.TRUE, scanner.FALSE:
		return BoolValue(tok == scanner.TRUE), nil
	default:
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"identifier", "string", "number", "bool"}, pos)
	}
}

// ParseIdent parses an identifier.
func (p *Parser) ParseIdent() (string, error) {
	tok, pos, lit := p.ScanIgnoreWhitespace()
	if tok != scanner.IDENT {
		return "", newParseError(scanner.Tokstr(tok, lit), []string{"identifier"}, pos)
	}
	return lit, nil
}

// ParseIdentList parses a comma delimited list of identifiers.
func (p *Parser) ParseIdentList() ([]string, error) {
	// Parse first (required) identifier.
	ident, err := p.ParseIdent()
	if err != nil {
		return nil, err
	}
	idents := []string{ident}

	// Parse remaining (optional) identifiers.
	for {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			return idents, nil
		}

		if ident, err = p.ParseIdent(); err != nil {
			return nil, err
		}

		idents = append(idents, ident)
	}
}

// parseParam parses a positional or named param.
func (p *Parser) parseParam() (interface{}, error) {
	tok, _, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.NAMEDPARAM:
		if len(lit) == 1 {
			return nil, &ParseError{Message: "missing param name"}
		}
		if p.orderedParams > 0 {
			return nil, &ParseError{Message: "can't mix positional arguments with named arguments"}
		}
		p.namedParams++
		return NamedParam(lit[1:]), nil
	case scanner.POSITIONALPARAM:
		if p.namedParams > 0 {
			return nil, &ParseError{Message: "can't mix positional arguments with named arguments"}
		}
		p.orderedParams++
		return PositionalParam(p.orderedParams), nil
	default:
		return nil, nil
	}
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
