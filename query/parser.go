package query

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Parser represents an Genji SQL parser.
type Parser struct {
	s *bufScanner
}

// NewParser returns a new instance of Parser.
func NewParser(r io.Reader) *Parser {
	return &Parser{s: newBufScanner(r)}
}

// ParseQuery parses a query string and returns its AST representation.
func ParseQuery(s string) (Query, error) { return NewParser(strings.NewReader(s)).ParseQuery() }

// ParseQuery parses an Genji SQL string and returns a Query.
func (p *Parser) ParseQuery() (Query, error) {
	var statements []Statement
	semi := true

	for {
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok == EOF {
			return New(statements...), nil
		} else if tok == SEMICOLON {
			semi = true
		} else {
			if !semi {
				return Query{}, newParseError(tokstr(tok, lit), []string{";"}, pos)
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
	case SELECT:
		return p.parseSelectStatement()
	case DELETE:
		return p.parseDeleteStatement()
	}

	return nil, newParseError(tokstr(tok, lit), []string{"SELECT", "DELETE"}, pos)
}

// parseSelectStatement parses a select string and returns a Statement AST object.
// This function assumes the SELECT token has already been consumed.
func (p *Parser) parseSelectStatement() (SelectStmt, error) {
	stmt := Select()

	// Parse "FROM".
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != FROM {
		return stmt, newParseError(tokstr(tok, lit), []string{"FROM"}, pos)
	}

	// Parse table name
	tableName, err := p.ParseIdent()
	if err != nil {
		return stmt, err
	}
	stmt = stmt.From(Table(tableName))

	// Parse condition: "WHERE EXPR".
	expr, err := p.parseCondition()
	if err != nil {
		return stmt, err
	}
	stmt = stmt.Where(expr)

	return stmt, nil
}

// parseDeleteStatement parses a delete string and returns a Statement AST object.
// This function assumes the DELETE token has already been consumed.
func (p *Parser) parseDeleteStatement() (DeleteStmt, error) {
	stmt := Delete()

	// Parse "FROM".
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != FROM {
		return stmt, newParseError(tokstr(tok, lit), []string{"FROM"}, pos)
	}

	// Parse table name
	tableName, err := p.ParseIdent()
	if err != nil {
		return stmt, err
	}
	stmt = stmt.From(Table(tableName))

	// Parse condition: "WHERE EXPR".
	expr, err := p.parseCondition()
	if err != nil {
		return stmt, err
	}
	stmt = stmt.Where(expr)

	return stmt, nil
}

// parseCondition parses the "WHERE" clause of the query, if it exists.
func (p *Parser) parseCondition() (Expr, error) {
	// Check if the WHERE token exists.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != WHERE {
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
	var root operator = &cmpOp{}

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
		if !op.isOperator() {
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

func opToExpr(op Token, lhs, rhs Expr) Expr {
	switch op {
	case EQ:
		return Eq(lhs, rhs)
	case GT:
		return Gt(lhs, rhs)
	case GTE:
		return Gte(lhs, rhs)
	case LT:
		return Lt(lhs, rhs)
	case LTE:
		return Lte(lhs, rhs)
	case AND:
		return And(lhs, rhs)
	case OR:
		return Or(lhs, rhs)
	}

	return nil
}

// parseUnaryExpr parses an non-binary expression.
func (p *Parser) parseUnaryExpr() (Expr, error) {
	tok, pos, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case IDENT:
		return Field(lit), nil
	case STRING:
		return StringValue(lit), nil
	case NUMBER:
		v, err := strconv.ParseFloat(lit, 64)
		if err != nil {
			return nil, &ParseError{Message: "unable to parse number", Pos: pos}
		}
		return Float64Value(v), nil
	case INTEGER:
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
	case TRUE, FALSE:
		return BoolValue(tok == TRUE), nil
	default:
		return nil, newParseError(tokstr(tok, lit), []string{"identifier", "string", "number", "bool"}, pos)
	}
}

// ParseIdent parses an identifier.
func (p *Parser) ParseIdent() (string, error) {
	tok, pos, lit := p.ScanIgnoreWhitespace()
	if tok != IDENT {
		return "", newParseError(tokstr(tok, lit), []string{"identifier"}, pos)
	}
	return lit, nil
}

// Scan returns the next token from the underlying scanner.
func (p *Parser) Scan() (tok Token, pos Pos, lit string) { return p.s.Scan() }

// ScanIgnoreWhitespace scans the next non-whitespace and non-comment token.
func (p *Parser) ScanIgnoreWhitespace() (tok Token, pos Pos, lit string) {
	for {
		tok, pos, lit = p.Scan()
		if tok == WS || tok == COMMENT {
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
	Pos      Pos
}

// newParseError returns a new instance of ParseError.
func newParseError(found string, expected []string, pos Pos) *ParseError {
	return &ParseError{Found: found, Expected: expected, Pos: pos}
}

// Error returns the string representation of the error.
func (e *ParseError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("%s at line %d, char %d", e.Message, e.Pos.Line+1, e.Pos.Char+1)
	}
	return fmt.Sprintf("found %s, expected %s at line %d, char %d", e.Found, strings.Join(e.Expected, ", "), e.Pos.Line+1, e.Pos.Char+1)
}
