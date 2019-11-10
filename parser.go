package genji

import (
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/asdine/genji/internal/scanner"
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
func parseQuery(s string) (query, error) { return newParser(strings.NewReader(s)).ParseQuery() }

// ParseQuery parses a Genji SQL string and returns a Query.
func (p *parser) ParseQuery() (query, error) {
	var statements []statement
	semi := true

	for {
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok == scanner.EOF {
			return newQuery(statements...), nil
		} else if tok == scanner.SEMICOLON {
			semi = true
		} else {
			if !semi {
				return query{}, newParseError(scanner.Tokstr(tok, lit), []string{";"}, pos)
			}
			p.Unscan()
			s, err := p.ParseStatement()
			if err != nil {
				return query{}, err
			}
			statements = append(statements, s)
			semi = false
		}
	}
}

// ParseStatement parses a Genji SQL string and returns a Statement AST object.
func (p *parser) ParseStatement() (statement, error) {
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
func (p *parser) parseCondition() (expr, error) {
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
	LeftHand() expr
	RightHand() expr
	SetLeftHandExpr(expr)
	SetRightHandExpr(expr)
}

// ParseExpr parses an expression.
func (p *parser) ParseExpr() (expr, error) {
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
		if !op.IsOperator() {
			p.Unscan()
			return root.RightHand(), nil
		}

		var rhs expr

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

func opToExpr(op scanner.Token, lhs, rhs expr) expr {
	switch op {
	case scanner.EQ:
		return eq(lhs, rhs)
	case scanner.GT:
		return gt(lhs, rhs)
	case scanner.GTE:
		return gte(lhs, rhs)
	case scanner.LT:
		return lt(lhs, rhs)
	case scanner.LTE:
		return lte(lhs, rhs)
	case scanner.AND:
		return and(lhs, rhs)
	case scanner.OR:
		return or(lhs, rhs)
	}

	return nil
}

// parseUnaryExpr parses an non-binary expression.
func (p *parser) parseUnaryExpr() (expr, error) {
	tok, pos, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.IDENT:
		p.stat.exprFields = append(p.stat.exprFields, lit)
		return fieldSelector(lit), nil
	case scanner.NAMEDPARAM:
		if len(lit) == 1 {
			return nil, &ParseError{Message: "missing param name"}
		}
		if p.orderedParams > 0 {
			return nil, &ParseError{Message: "can't mix positional arguments with named arguments"}
		}
		p.namedParams++
		return namedParam(lit[1:]), nil
	case scanner.POSITIONALPARAM:
		if p.namedParams > 0 {
			return nil, &ParseError{Message: "can't mix positional arguments with named arguments"}
		}
		p.orderedParams++
		return positionalParam(p.orderedParams), nil
	case scanner.STRING:
		return litteralValue{value.NewString(lit)}, nil
	case scanner.NUMBER:
		v, err := strconv.ParseFloat(lit, 64)
		if err != nil {
			return nil, &ParseError{Message: "unable to parse number", Pos: pos}
		}
		return litteralValue{value.NewFloat64(v)}, nil
	case scanner.INTEGER:
		v, err := strconv.ParseInt(lit, 10, 64)
		if err != nil {
			// The literal may be too large to fit into an int64. If it is, use an unsigned integer.
			// The check for negative numbers is handled somewhere else so this should always be a positive number.
			if v, err := strconv.ParseUint(lit, 10, 64); err == nil {
				return litteralValue{value.NewUint64(v)}, nil
			}
			return nil, &ParseError{Message: "unable to parse integer", Pos: pos}
		}
		switch {
		case v < math.MaxInt8:
			return litteralValue{value.NewInt8(int8(v))}, nil
		case v < math.MaxInt16:
			return litteralValue{value.NewInt16(int16(v))}, nil
		case v < math.MaxInt32:
			return litteralValue{value.NewInt32(int32(v))}, nil
		}
		return litteralValue{value.NewInt64(v)}, nil
	case scanner.TRUE, scanner.FALSE:
		return litteralValue{value.NewBool(tok == scanner.TRUE)}, nil
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
		return namedParam(lit[1:]), nil
	case scanner.POSITIONALPARAM:
		if p.namedParams > 0 {
			return nil, &ParseError{Message: "can't mix positional arguments with named arguments"}
		}
		p.orderedParams++
		return positionalParam(p.orderedParams), nil
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
