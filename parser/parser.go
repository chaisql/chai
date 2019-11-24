package parser

import (
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/asdine/genji/internal/scanner"
	"github.com/asdine/genji/query"
	"github.com/asdine/genji/value"
)

// parser represents an Genji SQL parser.
type parser struct {
	s             *scanner.BufScanner
	orderedParams int
	namedParams   int
	stat          parserStat
}

// newParser returns a new instance of Parser.
func newParser(r io.Reader) *parser {
	return &parser{s: scanner.NewBufScanner(r)}
}

// parseQuery parses a query string and returns its AST representation.
func parseQuery(s string) (query.Query, error) { return newParser(strings.NewReader(s)).ParseQuery() }

// ParseQuery parses a Genji SQL string and returns a Query.
func (p *parser) ParseQuery() (query.Query, error) {
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
func (p *parser) ParseStatement() (query.Statement, error) {
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
func (p *parser) parseCondition() (query.Expr, error) {
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
	LeftHand() query.Expr
	RightHand() query.Expr
	SetLeftHandExpr(query.Expr)
	SetRightHandExpr(query.Expr)
}

// ParseExpr parses an expression.
func (p *parser) ParseExpr() (query.Expr, error) {
	var err error
	// Dummy root node.
	var root operator = &query.CmpOp{}

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

		var rhs query.Expr

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

func opToExpr(op scanner.Token, lhs, rhs query.Expr) query.Expr {
	switch op {
	case scanner.EQ:
		return query.Eq(lhs, rhs)
	case scanner.NEQ:
		return query.Neq(lhs, rhs)
	case scanner.GT:
		return query.Gt(lhs, rhs)
	case scanner.GTE:
		return query.Gte(lhs, rhs)
	case scanner.LT:
		return query.Lt(lhs, rhs)
	case scanner.LTE:
		return query.Lte(lhs, rhs)
	case scanner.AND:
		return query.And(lhs, rhs)
	case scanner.OR:
		return query.Or(lhs, rhs)
	}

	panic(fmt.Sprintf("unknown operator %q", op))
}

// parseUnaryExpr parses an non-binary expression.
func (p *parser) parseUnaryExpr() (query.Expr, error) {
	tok, pos, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.IDENT:
		p.stat.exprFields = append(p.stat.exprFields, lit)
		return query.FieldSelector(lit), nil
	case scanner.NAMEDPARAM:
		if len(lit) == 1 {
			return nil, &ParseError{Message: "missing param name"}
		}
		if p.orderedParams > 0 {
			return nil, &ParseError{Message: "can't mix positional arguments with named arguments"}
		}
		p.namedParams++
		return query.NamedParam(lit[1:]), nil
	case scanner.POSITIONALPARAM:
		if p.namedParams > 0 {
			return nil, &ParseError{Message: "can't mix positional arguments with named arguments"}
		}
		p.orderedParams++
		return query.PositionalParam(p.orderedParams), nil
	case scanner.STRING:
		return query.LiteralValue{Value: value.NewString(lit)}, nil
	case scanner.NUMBER:
		v, err := strconv.ParseFloat(lit, 64)
		if err != nil {
			return nil, &ParseError{Message: "unable to parse number", Pos: pos}
		}
		return query.LiteralValue{Value: value.NewFloat64(v)}, nil
	case scanner.INTEGER:
		v, err := strconv.ParseInt(lit, 10, 64)
		if err != nil {
			// The literal may be too large to fit into an int64. If it is, use an unsigned integer.
			// The check for negative numbers is handled somewhere else so this should always be a positive number.
			if v, err := strconv.ParseUint(lit, 10, 64); err == nil {
				return query.LiteralValue{Value: value.NewUint64(v)}, nil
			}
			return nil, &ParseError{Message: "unable to parse integer", Pos: pos}
		}
		switch {
		case v < math.MaxInt8:
			return query.LiteralValue{Value: value.NewInt8(int8(v))}, nil
		case v < math.MaxInt16:
			return query.LiteralValue{Value: value.NewInt16(int16(v))}, nil
		case v < math.MaxInt32:
			return query.LiteralValue{Value: value.NewInt32(int32(v))}, nil
		}
		return query.LiteralValue{Value: value.NewInt64(v)}, nil
	case scanner.TRUE, scanner.FALSE:
		return query.LiteralValue{Value: value.NewBool(tok == scanner.TRUE)}, nil
	case scanner.NULL:
		return query.LiteralValue{Value: value.NewNull()}, nil
	default:
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"identifier", "string", "number", "bool"}, pos)
	}
}

// ParseIdent parses an identifier.
func (p *parser) ParseIdent() (string, error) {
	tok, pos, lit := p.ScanIgnoreWhitespace()
	if tok != scanner.IDENT {
		return "", newParseError(scanner.Tokstr(tok, lit), []string{"identifier"}, pos)
	}
	return lit, nil
}

// ParseIdentList parses a comma delimited list of identifiers.
func (p *parser) ParseIdentList() ([]string, error) {
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
func (p *parser) parseParam() (interface{}, error) {
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
		return query.NamedParam(lit[1:]), nil
	case scanner.POSITIONALPARAM:
		if p.namedParams > 0 {
			return nil, &ParseError{Message: "can't mix positional arguments with named arguments"}
		}
		p.orderedParams++
		return query.PositionalParam(p.orderedParams), nil
	default:
		return nil, nil
	}
}

func (p *parser) parseType() (value.Type, error) {
	tok, pos, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.TYPEBYTES:
		return value.Bytes, nil
	case scanner.TYPESTRING:
		return value.String, nil
	case scanner.TYPEBOOL:
		return value.Bool, nil
	case scanner.TYPEINT8:
		return value.Int8, nil
	case scanner.TYPEINT16:
		return value.Int16, nil
	case scanner.TYPEINT32:
		return value.Int32, nil
	case scanner.TYPEINT64:
		return value.Int64, nil
	case scanner.TYPEINT:
		return value.Int, nil
	case scanner.TYPEUINT8:
		return value.Uint8, nil
	case scanner.TYPEUINT16:
		return value.Uint16, nil
	case scanner.TYPEUINT32:
		return value.Uint32, nil
	case scanner.TYPEUINT64:
		return value.Uint64, nil
	case scanner.TYPEUINT:
		return value.Uint, nil
	case scanner.TYPEFLOAT64:
		return value.Float64, nil
	case scanner.TYPEINTEGER:
		return value.Int, nil
	case scanner.TYPENUMERIC:
		return value.Float64, nil
	case scanner.TYPETEXT:
		return value.String, nil
	}

	return 0, newParseError(scanner.Tokstr(tok, lit), []string{"type"}, pos)
}

// Scan returns the next token from the underlying scanner.
func (p *parser) Scan() (tok scanner.Token, pos scanner.Pos, lit string) { return p.s.Scan() }

// ScanIgnoreWhitespace scans the next non-whitespace and non-comment token.
func (p *parser) ScanIgnoreWhitespace() (tok scanner.Token, pos scanner.Pos, lit string) {
	for {
		tok, pos, lit = p.Scan()
		if tok == scanner.WS || tok == scanner.COMMENT {
			continue
		}
		return
	}
}

// Unscan pushes the previously read token back onto the buffer.
func (p *parser) Unscan() { p.s.Unscan() }

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
