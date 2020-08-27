package parser

import (
	"fmt"

	"github.com/genjidb/genji/sql/query"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/sql/scanner"
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
		pErr := err.(*ParseError)
		pErr.Expected = []string{"table_name"}
		return stmt, pErr
	}

	valueParser := p.parseParamOrDocument

	// Parse path list: (a, b, c)
	fields, withFields, err := p.parseFieldList()
	if err != nil {
		return stmt, err
	}
	if withFields {
		valueParser = p.parseParamOrExprList
		stmt.FieldNames = fields
	}

	// Parse VALUES (v1, v2, v3)
	values, err := p.parseValues(valueParser)
	if err != nil {
		return stmt, err
	}

	// ensure the length of path list is the same as the length of values
	if withFields {
		for _, l := range values {
			el := l.(expr.LiteralExprList)
			if len(el) != len(stmt.FieldNames) {
				return stmt, fmt.Errorf("%d values for %d fields", len(el), len(stmt.FieldNames))
			}
		}
	}

	stmt.Values = values
	return stmt, nil
}

// parseFieldList parses a list of fields in the form: (path, path, ...), if exists
func (p *Parser) parseFieldList() ([]string, bool, error) {
	// Parse ( token.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.LPAREN {
		p.Unscan()
		return nil, false, nil
	}

	// Parse path list.
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
func (p *Parser) parseValues(valueParser func() (expr.Expr, error)) (expr.LiteralExprList, error) {
	// Check if the VALUES token exists.
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.VALUES {
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"VALUES"}, pos)
	}

	var valuesList expr.LiteralExprList
	// Parse first (required) value list.
	d, err := valueParser()
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

		d, err := valueParser()
		if err != nil {
			return nil, err
		}

		valuesList = append(valuesList, d)
	}

	return valuesList, nil
}

// parseParamOrDocument parses either a parameter or a document.
func (p *Parser) parseParamOrDocument() (expr.Expr, error) {
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

	// Expect a document
	return p.parseDocument()
}

// parseParamOrExprList parses either a parameter or a list of expressions.
func (p *Parser) parseParamOrExprList() (expr.Expr, error) {
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

	// expect an expression list
	return p.parseExprList(scanner.LPAREN, scanner.RPAREN)
}
