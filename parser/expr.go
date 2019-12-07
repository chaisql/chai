package parser

import (
	"fmt"
	"math"
	"strconv"

	"github.com/asdine/genji/document"
	"github.com/asdine/genji/query"
	"github.com/asdine/genji/scanner"
)

type operator interface {
	Precedence() int
	LeftHand() query.Expr
	RightHand() query.Expr
	SetLeftHandExpr(query.Expr)
	SetRightHandExpr(query.Expr)
}

// ParseExpr parses an expression.
func (p *Parser) ParseExpr() (query.Expr, error) {
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
func (p *Parser) parseUnaryExpr() (query.Expr, error) {
	tok, pos, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.IDENT:
		p.Unscan()
		field, err := p.ParseField()
		if err != nil {
			return nil, err
		}
		fs := query.FieldSelector(field)
		p.stat.exprFields = append(p.stat.exprFields, fs.Name())
		return fs, nil
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
		return query.StringValue(lit), nil
	case scanner.NUMBER:
		v, err := strconv.ParseFloat(lit, 64)
		if err != nil {
			return nil, &ParseError{Message: "unable to parse number", Pos: pos}
		}
		return query.Float64Value(v), nil
	case scanner.INTEGER:
		v, err := strconv.ParseInt(lit, 10, 64)
		if err != nil {
			// The literal may be too large to fit into an int64. If it is, use an unsigned integer.
			if v, err := strconv.ParseUint(lit, 10, 64); err == nil {
				return query.Uint64Value(v), nil
			}
			return nil, &ParseError{Message: "unable to parse integer", Pos: pos}
		}
		switch {
		case v >= math.MinInt8 && v <= math.MaxInt8:
			return query.Int8Value(int8(v)), nil
		case v >= math.MinInt16 && v <= math.MaxInt16:
			return query.Int16Value(int16(v)), nil
		case v >= math.MinInt32 && v <= math.MaxInt32:
			return query.Int32Value(int32(v)), nil
		}
		return query.Int64Value(v), nil
	case scanner.TRUE, scanner.FALSE:
		return query.BoolValue(tok == scanner.TRUE), nil
	case scanner.NULL:
		return query.NullValue(), nil
	case scanner.LBRACKET:
		p.Unscan()
		e, _, err := p.parseDocument()
		return e, err
	case scanner.LSBRACKET:
		p.Unscan()
		return p.ParseExprList(scanner.LSBRACKET, scanner.RSBRACKET)
	case scanner.LPAREN:
		p.Unscan()
		return p.ParseExprList(scanner.LPAREN, scanner.RPAREN)
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
func (p *Parser) parseParam() (query.Expr, error) {
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

func (p *Parser) parseType() (document.ValueType, error) {
	tok, pos, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.TYPEBYTES:
		return document.BytesValue, nil
	case scanner.TYPESTRING:
		return document.StringValue, nil
	case scanner.TYPEBOOL:
		return document.BoolValue, nil
	case scanner.TYPEINT8:
		return document.Int8Value, nil
	case scanner.TYPEINT16:
		return document.Int16Value, nil
	case scanner.TYPEINT32:
		return document.Int32Value, nil
	case scanner.TYPEINT64:
		return document.Int64Value, nil
	case scanner.TYPEINT:
		return document.IntValue, nil
	case scanner.TYPEUINT8:
		return document.Uint8Value, nil
	case scanner.TYPEUINT16:
		return document.Uint16Value, nil
	case scanner.TYPEUINT32:
		return document.Uint32Value, nil
	case scanner.TYPEUINT64:
		return document.Uint64Value, nil
	case scanner.TYPEUINT:
		return document.UintValue, nil
	case scanner.TYPEFLOAT64:
		return document.Float64Value, nil
	case scanner.TYPEINTEGER:
		return document.IntValue, nil
	case scanner.TYPENUMERIC:
		return document.Float64Value, nil
	case scanner.TYPETEXT:
		return document.StringValue, nil
	}

	return 0, newParseError(scanner.Tokstr(tok, lit), []string{"type"}, pos)
}

// parseDocument parses a document
func (p *Parser) parseDocument() (query.Expr, bool, error) {
	// Parse { token.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.LBRACKET {
		p.Unscan()
		return nil, false, nil
	}

	var pairs query.KVPairs
	var pair query.KVPair
	var err error

	// Parse kv pairs.
	for {
		if pair, err = p.parseKV(); err != nil {
			p.Unscan()
			break
		}

		pairs = append(pairs, pair)

		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			break
		}
	}

	// Parse required } token.
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.RBRACKET {
		return nil, true, newParseError(scanner.Tokstr(tok, lit), []string{"}"}, pos)
	}

	return pairs, true, nil
}

// parseKV parses a key-value pair in the form IDENT : Expr.
func (p *Parser) parseKV() (query.KVPair, error) {
	var k string

	tok, pos, lit := p.ScanIgnoreWhitespace()
	if tok == scanner.IDENT || tok == scanner.STRING {
		k = lit
	} else {
		return query.KVPair{}, newParseError(scanner.Tokstr(tok, lit), []string{"ident", "string"}, pos)
	}

	tok, pos, lit = p.ScanIgnoreWhitespace()
	if tok != scanner.COLON {
		p.Unscan()
		return query.KVPair{}, newParseError(scanner.Tokstr(tok, lit), []string{":"}, pos)
	}

	expr, err := p.ParseExpr()
	if err != nil {
		return query.KVPair{}, err
	}

	return query.KVPair{
		K: k,
		V: expr,
	}, nil
}

// ParseField parses a field in the form ident(.ident)*
func (p *Parser) ParseField() ([]string, error) {
	var field []string
	// parse first mandatory ident
	chunk, err := p.ParseIdent()
	if err != nil {
		return nil, err
	}
	field = append(field, chunk)

	for {
		// scan the very next token
		if tok, _, _ := p.Scan(); tok != scanner.DOT {
			p.Unscan()
			return field, nil
		}

		chunk, err = p.ParseIdent()
		if err != nil {
			return nil, err
		}

		field = append(field, chunk)
	}
}

func (p *Parser) ParseExprList(leftToken, rightToken scanner.Token) (query.LiteralExprList, error) {
	// Parse ( or [ token.
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != leftToken {
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{leftToken.String()}, pos)
	}

	var exprList query.LiteralExprList
	var expr query.Expr
	var err error

	// Parse kv pairs.
	for {
		if expr, err = p.ParseExpr(); err != nil {
			p.Unscan()
			break
		}

		exprList = append(exprList, expr)

		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			break
		}
	}

	// Parse required ) or ] token.
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != rightToken {
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{rightToken.String()}, pos)
	}

	return exprList, nil
}
