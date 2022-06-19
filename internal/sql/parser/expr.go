package parser

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/expr/functions"
	"github.com/genjidb/genji/internal/sql/scanner"
	"github.com/genjidb/genji/types"
)

type dummyOperator struct {
	rightHand expr.Expr
}

func (d *dummyOperator) Token() scanner.Token { panic("not implemented") }
func (d *dummyOperator) Equal(expr.Expr) bool { panic("not implemented") }
func (d *dummyOperator) Eval(*environment.Environment) (types.Value, error) {
	panic("not implemented")
}
func (d *dummyOperator) String() string               { panic("not implemented") }
func (d *dummyOperator) Precedence() int              { panic("not implemented") }
func (d *dummyOperator) LeftHand() expr.Expr          { panic("not implemented") }
func (d *dummyOperator) RightHand() expr.Expr         { return d.rightHand }
func (d *dummyOperator) SetLeftHandExpr(e expr.Expr)  { panic("not implemented") }
func (d *dummyOperator) SetRightHandExpr(e expr.Expr) { d.rightHand = e }

// ParseExpr parses an expression.
func (p *Parser) ParseExpr() (e expr.Expr, err error) {
	return p.parseExprWithMinPrecedence(0)
}

func (p *Parser) parseExprWithMinPrecedence(precedence int, allowed ...scanner.Token) (e expr.Expr, err error) {
	// Dummy root node.
	var root expr.Operator = new(dummyOperator)

	// Parse a non-binary expression type to start.
	// This variable will always be the root of the expression tree.
	e, err = p.parseUnaryExpr(allowed...)
	if err != nil {
		return nil, err
	}
	root.SetRightHandExpr(e)

	// Loop over operations and unary exprs and build a tree based on precedence.
	for {
		// If the next token is NOT an operator then return the expression.
		op, tok, err := p.parseOperator(precedence, allowed...)
		if err != nil {
			return nil, err
		}
		if tok == 0 {
			return root.RightHand(), nil
		}

		var rhs expr.Expr

		if rhs, err = p.parseUnaryExpr(allowed...); err != nil {
			return nil, err
		}

		// Find the right spot in the tree to add the new expression by
		// descending the RHS of the expression tree until we reach the last
		// BinaryExpr or a BinaryExpr whose RHS has an operator with
		// precedence >= the operator being added.
		for node := root; ; {
			p, ok := node.RightHand().(expr.Operator)
			if !ok || p.Precedence() >= tok.Precedence() {
				// Add the new expression here and break.
				node.SetRightHandExpr(op(node.RightHand(), rhs))
				break
			}
			node = p
		}
	}
}

func (p *Parser) parseOperator(minPrecedence int, allowed ...scanner.Token) (func(lhs, rhs expr.Expr) expr.Expr, scanner.Token, error) {
	op, _, _ := p.ScanIgnoreWhitespace()
	if !op.IsOperator() && op != scanner.NOT {
		p.Unscan()
		return nil, 0, nil
	}

	if !tokenIsAllowed(op, allowed...) {
		p.Unscan()
		return nil, 0, nil
	}

	// Ignore currently unused operators.
	if op == scanner.EQREGEX || op == scanner.NEQREGEX {
		p.Unscan()
		return nil, 0, nil
	}

	if op == scanner.NOT {
		tok, pos, lit := p.ScanIgnoreWhitespace()
		if tok.Precedence() >= minPrecedence {
			switch {
			case tok == scanner.IN && tok.Precedence() >= minPrecedence:
				return expr.NotIn, scanner.NIN, nil
			case tok == scanner.LIKE && tok.Precedence() >= minPrecedence:
				return expr.NotLike, scanner.NLIKE, nil
			}
		}

		return nil, 0, newParseError(scanner.Tokstr(tok, lit), []string{"IN, LIKE"}, pos)
	}

	if op.Precedence() < minPrecedence {
		p.Unscan()
		return nil, 0, nil
	}

	switch op {
	case scanner.EQ:
		return expr.Eq, op, nil
	case scanner.NEQ:
		return expr.Neq, op, nil
	case scanner.GT:
		return expr.Gt, op, nil
	case scanner.GTE:
		return expr.Gte, op, nil
	case scanner.LT:
		return expr.Lt, op, nil
	case scanner.LTE:
		return expr.Lte, op, nil
	case scanner.AND:
		return expr.And, op, nil
	case scanner.OR:
		return expr.Or, op, nil
	case scanner.ADD:
		return expr.Add, op, nil
	case scanner.SUB:
		return expr.Sub, op, nil
	case scanner.MUL:
		return expr.Mul, op, nil
	case scanner.DIV:
		return expr.Div, op, nil
	case scanner.MOD:
		return expr.Mod, op, nil
	case scanner.BITWISEAND:
		return expr.BitwiseAnd, op, nil
	case scanner.BITWISEOR:
		return expr.BitwiseOr, op, nil
	case scanner.BITWISEXOR:
		return expr.BitwiseXor, op, nil
	case scanner.IN:
		return expr.In, op, nil
	case scanner.IS:
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.NOT {
			return expr.IsNot, scanner.ISN, nil
		}
		p.Unscan()
		return expr.Is, op, nil
	case scanner.LIKE:
		return expr.Like, op, nil
	case scanner.CONCAT:
		return expr.Concat, op, nil
	case scanner.BETWEEN:
		a, err := p.parseExprWithMinPrecedence(op.Precedence())
		if err != nil {
			return nil, op, err
		}
		err = p.parseTokens(scanner.AND)
		if err != nil {
			return nil, op, err
		}

		return expr.Between(a), op, nil
	}

	p.Unscan()

	return nil, 0, nil
}

// parseUnaryExpr parses an non-binary expression.
func (p *Parser) parseUnaryExpr(allowed ...scanner.Token) (expr.Expr, error) {
	tok, pos, lit := p.ScanIgnoreWhitespace()

	if !tokenIsAllowed(tok, allowed...) {
		p.Unscan()
		return nil, nil
	}

	switch tok {
	case scanner.CAST:
		p.Unscan()
		return p.parseCastExpression()
	case scanner.IDENT:
		tok1, _, _ := p.ScanIgnoreWhitespace()
		// if the next token is a left parenthesis, this is a global function
		if tok1 == scanner.LPAREN {
			p.Unscan()
			if tk, _, _ := p.s.Curr(); tk == scanner.WS {
				p.Unscan()
			}
			p.Unscan()
			return p.parseFunction()
		} else if tok1 == scanner.DOT {
			// it may be a package function instead.
			if tok2, _, _ := p.Scan(); tok2 == scanner.IDENT {
				if tok3, _, _ := p.Scan(); tok3 == scanner.LPAREN {
					p.Unscan()
					p.Unscan()
					p.Unscan()
					p.Unscan()
					return p.parseFunction()
				} else {
					p.Unscan()
					p.Unscan()
				}
			} else {
				p.Unscan()
			}
		}
		p.Unscan()
		if tk, _, _ := p.s.Curr(); tk == scanner.WS {
			p.Unscan()
		}

		p.Unscan()

		field, err := p.parsePath()
		if err != nil {
			return nil, err
		}
		fs := expr.Path(field)
		return fs, nil
	case scanner.NAMEDPARAM:
		if len(lit) == 1 {
			return nil, errors.WithStack(&ParseError{Message: "missing param name"})
		}
		if p.orderedParams > 0 {
			return nil, errors.WithStack(&ParseError{Message: "cannot mix positional arguments with named arguments"})
		}
		p.namedParams++
		return expr.NamedParam(lit[1:]), nil
	case scanner.POSITIONALPARAM:
		if p.namedParams > 0 {
			return nil, errors.WithStack(&ParseError{Message: "cannot mix positional arguments with named arguments"})
		}
		p.orderedParams++
		return expr.PositionalParam(p.orderedParams), nil
	case scanner.STRING:
		if strings.HasPrefix(lit, `\x`) {
			blob, err := hex.DecodeString(lit[2:])
			if err != nil {
				if bt, ok := err.(hex.InvalidByteError); ok {
					return nil, fmt.Errorf("invalid hexadecimal digit: %c", bt)
				}

				return nil, err
			}
			return expr.LiteralValue{Value: types.NewBlobValue(blob)}, nil
		}
		return expr.LiteralValue{Value: types.NewTextValue(lit)}, nil
	case scanner.NUMBER:
		v, err := strconv.ParseFloat(lit, 64)
		if err != nil {
			return nil, errors.WithStack(&ParseError{Message: "unable to parse number", Pos: pos})
		}
		return expr.LiteralValue{Value: types.NewDoubleValue(v)}, nil
	case scanner.ADD, scanner.SUB:
		sign := tok
		tok, pos, lit = p.Scan()
		if tok != scanner.NUMBER && tok != scanner.INTEGER {
			return nil, errors.WithStack(&ParseError{Message: "syntax error", Pos: pos})
		}
		if sign == scanner.SUB {
			lit = "-" + lit
		}
		fallthrough
	case scanner.INTEGER:
		v, err := strconv.ParseInt(lit, 10, 64)
		if err != nil {
			// The literal may be too large to fit into an int64, parse as Float64
			if v, err := strconv.ParseFloat(lit, 64); err == nil {
				return expr.LiteralValue{Value: types.NewDoubleValue(v)}, nil
			}
			return nil, errors.WithStack(&ParseError{Message: "unable to parse integer", Pos: pos})
		}
		return expr.LiteralValue{Value: types.NewIntegerValue(v)}, nil
	case scanner.TRUE, scanner.FALSE:
		return expr.LiteralValue{Value: types.NewBoolValue(tok == scanner.TRUE)}, nil
	case scanner.NULL:
		return expr.LiteralValue{Value: types.NewNullValue()}, nil
	case scanner.LBRACKET:
		p.Unscan()
		e, err := p.ParseDocument()
		return e, err
	case scanner.LSBRACKET:
		p.Unscan()
		return p.parseExprList(scanner.LSBRACKET, scanner.RSBRACKET)
	case scanner.LPAREN:
		e, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}

		tok, pos, lit := p.ScanIgnoreWhitespace()
		switch tok {
		case scanner.RPAREN:
			return expr.Parentheses{E: e}, nil
		case scanner.COMMA:
			exprList, err := p.parseExprListUntil(scanner.RPAREN)
			if err != nil {
				return nil, err
			}

			// prepend first parsed expression
			exprList = append([]expr.Expr{e}, exprList...)
			return exprList, nil
		}

		return nil, newParseError(scanner.Tokstr(tok, lit), []string{")", ","}, pos)
	case scanner.NOT:
		e, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}
		return expr.Not(e), nil
	case scanner.NEXT:
		err := p.parseTokens(scanner.VALUE, scanner.FOR)
		if err != nil {
			return nil, err
		}
		seqName, err := p.parseIdent()
		if err != nil {
			return nil, err
		}

		return expr.NextValueFor{SeqName: seqName}, nil
	default:
		return nil, newParseError(scanner.Tokstr(tok, lit), nil, pos)
	}
}

// parseInteger parses an integer.
func (p *Parser) parseInteger() (int64, error) {
	tok, pos, lit := p.ScanIgnoreWhitespace()

	if tok == scanner.ADD || tok == scanner.SUB {
		sign := tok
		tok, pos, lit = p.Scan()
		if sign == scanner.SUB {
			lit = "-" + lit
		}
	}

	if tok != scanner.INTEGER {
		return 0, newParseError(scanner.Tokstr(tok, lit), []string{"integer"}, pos)
	}

	v, err := strconv.ParseInt(lit, 10, 64)
	if err != nil {
		return 0, newParseError(scanner.Tokstr(tok, lit), []string{"INT"}, pos)
	}

	return v, nil
}

// parseIdent parses an identifier.
func (p *Parser) parseIdent() (string, error) {
	tok, pos, lit := p.ScanIgnoreWhitespace()
	if tok != scanner.IDENT {
		return "", newParseError(scanner.Tokstr(tok, lit), []string{"identifier"}, pos)
	}

	return lit, nil
}

// parseIdentList parses a comma delimited list of identifiers.
func (p *Parser) parseIdentList() ([]string, error) {
	// Parse first (required) identifier.
	ident, err := p.parseIdent()
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

		if ident, err = p.parseIdent(); err != nil {
			return nil, err
		}

		idents = append(idents, ident)
	}
}

// parseParam parses a positional or named param.
func (p *Parser) parseParam() (expr.Expr, error) {
	tok, _, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.NAMEDPARAM:
		if len(lit) == 1 {
			return nil, errors.WithStack(&ParseError{Message: "missing param name"})
		}
		if p.orderedParams > 0 {
			return nil, errors.WithStack(&ParseError{Message: "cannot mix positional arguments with named arguments"})
		}
		p.namedParams++
		return expr.NamedParam(lit[1:]), nil
	case scanner.POSITIONALPARAM:
		if p.namedParams > 0 {
			return nil, errors.WithStack(&ParseError{Message: "cannot mix positional arguments with named arguments"})
		}
		p.orderedParams++
		return expr.PositionalParam(p.orderedParams), nil
	default:
		return nil, nil
	}
}

func (p *Parser) parseType() (types.ValueType, error) {
	tok, pos, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.TYPEANY:
		return types.AnyValue, nil
	case scanner.TYPEARRAY:
		return types.ArrayValue, nil
	case scanner.TYPEBLOB, scanner.TYPEBYTES:
		return types.BlobValue, nil
	case scanner.TYPEBOOL, scanner.TYPEBOOLEAN:
		return types.BooleanValue, nil
	case scanner.TYPEDOCUMENT:
		return types.DocumentValue, nil
	case scanner.TYPEREAL:
		return types.DoubleValue, nil
	case scanner.TYPEDOUBLE:
		tok, _, _ := p.ScanIgnoreWhitespace()
		if tok == scanner.PRECISION {
			return types.DoubleValue, nil
		}
		p.Unscan()
		return types.DoubleValue, nil
	case scanner.TYPEINTEGER, scanner.TYPEINT, scanner.TYPEINT2, scanner.TYPEINT8, scanner.TYPETINYINT,
		scanner.TYPEBIGINT, scanner.TYPEMEDIUMINT, scanner.TYPESMALLINT:
		return types.IntegerValue, nil
	case scanner.TYPETEXT:
		return types.TextValue, nil
	case scanner.TYPEVARCHAR, scanner.TYPECHARACTER:
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.LPAREN {
			return 0, newParseError(scanner.Tokstr(tok, lit), []string{"("}, pos)
		}

		// The value between parentheses is not used.
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.INTEGER {
			return 0, newParseError(scanner.Tokstr(tok, lit), []string{"integer"}, pos)
		}

		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.RPAREN {
			return 0, newParseError(scanner.Tokstr(tok, lit), []string{")"}, pos)
		}

		return types.TextValue, nil
	}

	return 0, newParseError(scanner.Tokstr(tok, lit), []string{"type"}, pos)
}

// ParseDocument parses a document
func (p *Parser) ParseDocument() (*expr.KVPairs, error) {
	// Parse { token.
	if err := p.parseTokens(scanner.LBRACKET); err != nil {
		return nil, err
	}

	var pairs expr.KVPairs
	pairs.SelfReferenced = true
	var pair expr.KVPair
	var err error

	// Parse kv pairs.
	for {
		if pair, err = p.parseKV(); err != nil {
			p.Unscan()
			break
		}

		pairs.Pairs = append(pairs.Pairs, pair)

		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			break
		}
	}

	// Parse required } token.
	if err := p.parseTokens(scanner.RBRACKET); err != nil {
		return nil, err
	}

	return &pairs, nil
}

// parseKV parses a key-value pair in the form IDENT : Expr.
func (p *Parser) parseKV() (expr.KVPair, error) {
	var k string

	tok, pos, lit := p.ScanIgnoreWhitespace()
	if tok == scanner.IDENT || tok == scanner.STRING {
		k = lit
	} else {
		return expr.KVPair{}, newParseError(scanner.Tokstr(tok, lit), []string{"ident", "string"}, pos)
	}

	if err := p.parseTokens(scanner.COLON); err != nil {
		p.Unscan()
		return expr.KVPair{}, err
	}

	e, err := p.ParseExpr()
	if err != nil {
		return expr.KVPair{}, err
	}

	return expr.KVPair{
		K: k,
		V: e,
	}, nil
}

// parsePath parses a path to a specific value.
func (p *Parser) parsePath() (document.Path, error) {
	var path document.Path
	// parse first mandatory ident
	chunk, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	path = append(path, document.PathFragment{
		FieldName: chunk,
	})

LOOP:
	for {
		// scan the very next token.
		// if can be either a '.' or a '['
		// Otherwise, unscan and return the path
		tok, _, _ := p.Scan()
		switch tok {
		case scanner.DOT:
			// scan the next token for an ident
			tok, pos, lit := p.Scan()
			if tok != scanner.IDENT {
				return nil, newParseError(lit, []string{"identifier"}, pos)
			}
			path = append(path, document.PathFragment{
				FieldName: lit,
			})
		case scanner.LSBRACKET:
			// the next token can be either an integer or a quoted string
			// if it's an integer, we have an array index
			// if it's a quoted string, we have a field name
			tok, pos, lit := p.Scan()
			switch tok {
			case scanner.INTEGER:
				// is the number negative?
				if lit[0] == '-' {
					return nil, newParseError(lit, []string{"integer"}, pos)
				}
				// is the number too big?
				if len(lit) > 10 {
					return nil, newParseError(lit, []string{"integer"}, pos)
				}
				// parse the integer
				i, err := strconv.ParseInt(lit, 10, 64)
				if err != nil {
					return nil, newParseError(lit, []string{"integer"}, pos)
				}
				path = append(path, document.PathFragment{
					ArrayIndex: int(i),
				})
			case scanner.STRING:
				path = append(path, document.PathFragment{
					FieldName: lit,
				})
			}
			// scan the next token for a closing left bracket
			if err := p.parseTokens(scanner.RSBRACKET); err != nil {
				return nil, err
			}
		default:
			p.Unscan()
			break LOOP
		}
	}

	return path, nil
}

func (p *Parser) parseExprListUntil(rightToken scanner.Token) (expr.LiteralExprList, error) {
	var exprList expr.LiteralExprList
	var expr expr.Expr
	var err error

	// Parse expressions.
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
	if err := p.parseTokens(rightToken); err != nil {
		return nil, err
	}

	return exprList, nil
}

func (p *Parser) parseExprList(leftToken, rightToken scanner.Token) (expr.LiteralExprList, error) {
	// Parse ( or [ token.
	if err := p.parseTokens(leftToken); err != nil {
		return nil, err
	}

	return p.parseExprListUntil(rightToken)
}

// parseFunction parses a function call.
// a function is an identifier followed by a parenthesis,
// an optional coma-separated list of expressions and a closing parenthesis.
func (p *Parser) parseFunction() (expr.Expr, error) {
	// Parse function name.
	funcName, err := p.parseIdent()
	if err != nil {
		return nil, err
	}

	// Parse optional package name
	var pkgName string
	if tok, _, _ := p.Scan(); tok == scanner.DOT {
		pkgName = funcName
		funcName, err = p.parseIdent()
		if err != nil {
			return nil, err
		}
	} else {
		p.Unscan()
	}

	// Parse required ( token.
	if err := p.parseTokens(scanner.LPAREN); err != nil {
		return nil, err
	}

	// Special case: If the function is COUNT, support the special case COUNT(*)
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok == scanner.MUL {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.RPAREN {
			return nil, newParseError(scanner.Tokstr(tok, lit), []string{")"}, pos)
		}

		return &functions.Count{Wildcard: true}, nil
	}
	p.Unscan()

	// Check if the function is called without arguments.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.RPAREN {
		def, err := p.packagesTable.GetFunc(pkgName, funcName)
		if err != nil {
			return nil, err
		}
		return def.Function()
	}
	p.Unscan()

	var exprs []expr.Expr

	// Parse expressions.
	for {
		e, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}

		exprs = append(exprs, e)

		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			break
		}
	}

	// Parse required ) token.
	if err := p.parseTokens(scanner.RPAREN); err != nil {
		return nil, err
	}

	def, err := p.packagesTable.GetFunc(pkgName, funcName)
	if err != nil {
		return nil, err
	}
	return def.Function(exprs...)
}

// parseCastExpression parses a string of the form CAST(expr AS type).
func (p *Parser) parseCastExpression() (expr.Expr, error) {
	// Parse required CAST and ( tokens.
	if err := p.parseTokens(scanner.CAST, scanner.LPAREN); err != nil {
		return nil, err
	}

	// parse required expression.
	e, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}

	// Parse required AS token.
	if err := p.parseTokens(scanner.AS); err != nil {
		return nil, err
	}

	// Parse required typename.
	tp, err := p.parseType()
	if err != nil {
		return nil, err
	}

	// Parse required ) token.
	if err := p.parseTokens(scanner.RPAREN); err != nil {
		return nil, err
	}

	return expr.Cast{Expr: e, CastAs: tp}, nil
}

// tokenIsAllowed is a helper function that determines if a token is allowed.
func tokenIsAllowed(tok scanner.Token, allowed ...scanner.Token) bool {
	if allowed == nil {
		return true
	}
	for _, a := range allowed {
		if tok == a {
			return true
		}
	}
	return false
}
