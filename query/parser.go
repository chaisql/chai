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
	case UPDATE:
		return p.parseUpdateStatement()
	case INSERT:
		return p.parseInsertStatement()
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

// parseUpdateStatement parses a update string and returns a Statement AST object.
// This function assumes the UPDATE token has already been consumed.
func (p *Parser) parseUpdateStatement() (UpdateStmt, error) {
	stmt := UpdateStmt{
		pairs: make(map[string]Expr),
	}

	// Parse table name
	tableName, err := p.ParseIdent()
	if err != nil {
		return stmt, err
	}
	stmt.tableSelector = Table(tableName)

	// Parse assignment: "SET field = EXPR".
	stmt.pairs, err = p.parseSetClause()
	if err != nil {
		return stmt, err
	}

	// Parse condition: "WHERE EXPR".
	where, err := p.parseCondition()
	if err != nil {
		return stmt, err
	}
	stmt = stmt.Where(where)

	return stmt, nil
}

// parseInsertStatement parses an insert string and returns a Statement AST object.
// This function assumes the INSERT token has already been consumed.
func (p *Parser) parseInsertStatement() (InsertStmt, error) {
	stmt := Insert()

	// Parse "INTO".
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != INTO {
		return stmt, newParseError(tokstr(tok, lit), []string{"INTO"}, pos)
	}

	// Parse table name
	tableName, err := p.ParseIdent()
	if err != nil {
		return stmt, err
	}
	stmt = stmt.Into(Table(tableName))

	fields, ok, err := p.parseFieldList()
	if err != nil {
		return stmt, err
	}
	if ok {
		stmt = stmt.Fields(fields...)
	}

	values, err := p.parseValues()
	if err != nil {
		return stmt, err
	}
	stmt = stmt.Values(values...)

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

// parseSetClause parses the "SET" clause of the query.
func (p *Parser) parseSetClause() (map[string]Expr, error) {
	// Check if the SET token exists.
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != SET {
		return nil, newParseError(tokstr(tok, lit), []string{"SET"}, pos)
	}

	pairs := make(map[string]Expr)

	firstPair := true
	for {
		if !firstPair {
			// Scan for a comma.
			tok, _, _ := p.ScanIgnoreWhitespace()
			if tok != COMMA {
				p.Unscan()
				break
			}
		}

		// Scan the identifier for the field name.
		tok, pos, lit := p.ScanIgnoreWhitespace()
		if tok != IDENT {
			return nil, newParseError(tokstr(tok, lit), []string{"identifier"}, pos)
		}

		// Scan the eq sign
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != EQ {
			return nil, newParseError(tokstr(tok, lit), []string{"="}, pos)
		}

		// Scan the expr for the value.
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}
		pairs[lit] = expr

		firstPair = false
	}

	return pairs, nil
}

// parseValues parses the "VALUES" clause of the query, if it exists.
func (p *Parser) parseValues() ([]Expr, error) {
	// Check if the VALUES token exists.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != VALUES {
		p.Unscan()
		return nil, nil
	}

	// Scan the identifier for the source.
	expr, err := p.parseExprList()
	if err != nil {
		return nil, err
	}

	return expr, nil
}

// parseFieldList parses a list of fields in the form: (field, field, ...), if exists
func (p *Parser) parseFieldList() ([]string, bool, error) {
	// Parse ( token.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != LPAREN {
		p.Unscan()
		return nil, false, nil
	}

	// Parse field list.
	var fields []string
	var err error
	if fields, err = p.ParseIdentList(); err != nil {
		return nil, false, err
	}

	// Parse required ) token.
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != RPAREN {
		return nil, false, newParseError(tokstr(tok, lit), []string{")"}, pos)
	}

	return fields, true, nil
}

// parseExprList parses a list of expressions in the form: (expr, expr, ...)
func (p *Parser) parseExprList() ([]Expr, error) {
	// Parse ( token.
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != LPAREN {
		return nil, newParseError(tokstr(tok, lit), []string{"("}, pos)
	}

	// Parse first (required) expr.
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}
	exprs := []Expr{expr}

	// Parse remaining (optional) exprs.
	for {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != COMMA {
			p.Unscan()
			break
		}

		if expr, err = p.ParseExpr(); err != nil {
			return nil, err
		}

		exprs = append(exprs, expr)
	}

	// Parse required ) token.
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != RPAREN {
		return nil, newParseError(tokstr(tok, lit), []string{")"}, pos)
	}

	return exprs, nil
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
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != COMMA {
			p.Unscan()
			return idents, nil
		}

		if ident, err = p.ParseIdent(); err != nil {
			return nil, err
		}

		idents = append(idents, ident)
	}
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
