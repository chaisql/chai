package parser

import (
	"github.com/asdine/genji/query"
	"github.com/asdine/genji/scanner"
)

// parseInsertStatement parses an insert string and returns a Statement AST object.
// This function assumes the INSERT token has already been consumed.
func (p *Parser) parseInsertStatement() (query.InsertStmt, error) {
	var stmt query.InsertStmt
	var err error

	// Parse "INTO".
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.INTO {
		return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"INTO"}, pos)
	}

	// Parse table name
	stmt.TableName, err = p.ParseIdent()
	if err != nil {
		return stmt, err
	}

	// Parse field list: (a, b, c)
	fields, ok, err := p.parseFieldList()
	if err != nil {
		return stmt, err
	}
	if ok {
		stmt.FieldNames = fields
	}

	// Parse VALUES (v1, v2, v3)
	stmt.Values, err = p.parseValues()
	if err != nil {
		return stmt, err
	}

	return stmt, nil
}

// parseFieldList parses a list of fields in the form: (field, field, ...), if exists
func (p *Parser) parseFieldList() ([]string, bool, error) {
	// Parse ( token.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.LPAREN {
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
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.RPAREN {
		return nil, false, newParseError(scanner.Tokstr(tok, lit), []string{")"}, pos)
	}

	return fields, true, nil
}

// parseValues parses the "VALUES" clause of the query, if it exists.
func (p *Parser) parseValues() (query.LiteralExprList, error) {
	// Check if the VALUES token exists.
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.VALUES {
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"VALUES"}, pos)
	}

	var valuesList query.LiteralExprList
	// Parse first (required) value list.
	d, err := p.parseValue()
	if err != nil {
		return nil, err
	}

	valuesList = append(valuesList, d)

	// Parse remaining (optional) values.
	for {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			break
		}

		d, err := p.parseValue()
		if err != nil {
			return nil, err
		}

		valuesList = append(valuesList, d)
	}

	return valuesList, nil
}

// parseValue parses either a parameter, a JSON document or a list of expressions.
func (p *Parser) parseValue() (query.Expr, error) {
	// Parse a param first
	prm, err := p.parseParam()
	if err != nil {
		return nil, err
	}
	if prm != nil {
		return prm, nil
	}

	// If not a param, start over
	p.Unscan()

	// check if it's a json document
	expr, ok, err := p.parseDocument()
	if err != nil || ok {
		return expr, err
	}

	// if not a document, start over
	p.Unscan()

	// check if it's an expression list
	expr, ok, err = p.parseExprList()
	if err != nil {
		return nil, err
	}
	if !ok {
		tok, pos, lit := p.ScanIgnoreWhitespace()
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"expression list or JSON"}, pos)
	}

	return expr, nil
}

// parseExprList parses a list of expressions in the form: (expr, expr, ...)
func (p *Parser) parseExprList() (query.Expr, bool, error) {
	// Parse ( token.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.LPAREN {
		p.Unscan()
		return nil, false, nil
	}

	// Parse first (required) expr.
	e, err := p.ParseExpr()
	if err != nil {
		return nil, true, err
	}
	exprs := []query.Expr{e}

	// Parse remaining (optional) exprs.
	for {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			break
		}

		if e, err = p.ParseExpr(); err != nil {
			return nil, true, err
		}

		exprs = append(exprs, e)
	}

	// Parse required ) token.
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.RPAREN {
		return nil, true, newParseError(scanner.Tokstr(tok, lit), []string{")"}, pos)
	}

	return query.LiteralExprList(exprs), true, nil
}
