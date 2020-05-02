package parser

import (
	"github.com/asdine/genji/sql/query"
	"github.com/asdine/genji/sql/query/expr"
	"github.com/asdine/genji/sql/scanner"
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
	stmt.TableName, err = p.parseIdent()
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
	if fields, err = p.parseIdentList(); err != nil {
		return nil, false, err
	}

	// Parse required ) token.
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.RPAREN {
		return nil, false, newParseError(scanner.Tokstr(tok, lit), []string{")"}, pos)
	}

	return fields, true, nil
}

// parseValues parses the "VALUES" clause of the query, if it exists.
func (p *Parser) parseValues() (expr.LiteralExprList, error) {
	// Check if the VALUES token exists.
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.VALUES {
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"VALUES"}, pos)
	}

	var valuesList expr.LiteralExprList
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
func (p *Parser) parseValue() (expr.Expr, error) {
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
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.LPAREN {
		tok, pos, lit := p.ScanIgnoreWhitespace()
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"expression list or JSON"}, pos)
	}
	p.Unscan()
	expr, err = p.parseExprList(scanner.LPAREN, scanner.RPAREN)
	if err != nil {
		return nil, err
	}

	return expr, nil
}
