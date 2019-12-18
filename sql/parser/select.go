package parser

import (
	"github.com/asdine/genji/sql/query"
	"github.com/asdine/genji/sql/scanner"
)

// parseSelectStatement parses a select string and returns a Statement AST object.
// This function assumes the SELECT token has already been consumed.
func (p *Parser) parseSelectStatement() (query.SelectStmt, error) {
	var stmt query.SelectStmt
	var err error

	// Parse field list or query.Wildcard
	stmt.Selectors, err = p.parseResultFields()
	if err != nil {
		return stmt, err
	}

	// Parse "FROM".
	stmt.TableName, err = p.parseFrom()
	if err != nil {
		return stmt, err
	}

	// Parse condition: "WHERE EXPR".
	stmt.WhereExpr, err = p.parseCondition()
	if err != nil {
		return stmt, err
	}

	// Parse order by: "ORDER BY fieldRef [ASC|DESC]?"
	stmt.OrderBy, stmt.OrderByDirection, err = p.parseOrderBy()
	if err != nil {
		return stmt, err
	}

	// Parse limit: "LIMIT EXPR"
	stmt.LimitExpr, err = p.parseLimit()
	if err != nil {
		return stmt, err
	}

	// Parse offset: "OFFSET EXPR"
	stmt.OffsetExpr, err = p.parseOffset()
	if err != nil {
		return stmt, err
	}

	return stmt, nil
}

// parseResultFields parses the list of result fields.
func (p *Parser) parseResultFields() ([]query.ResultField, error) {
	// Parse first (required) result field.
	rf, err := p.parseResultField()
	if err != nil {
		return nil, err
	}
	rfields := []query.ResultField{rf}

	// Parse remaining (optional) result fields.
	for {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			return rfields, nil
		}

		if rf, err = p.parseResultField(); err != nil {
			return nil, err
		}

		rfields = append(rfields, rf)
	}
}

// parseResultField parses the list of result fields.
func (p *Parser) parseResultField() (query.ResultField, error) {
	// Check if the * token exists.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.MUL {
		return query.Wildcard{}, nil
	}
	p.Unscan()

	// Check if it's the key() function
	tok, pos, lit := p.ScanIgnoreWhitespace()
	if tok == scanner.KEY {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.LPAREN {
			if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.RPAREN {
				return nil, newParseError(scanner.Tokstr(tok, lit), []string{")"}, pos)
			}

			return query.KeyFunc{}, nil
		}
	}
	p.Unscan()

	field, err := p.ParseFieldRef()
	if err != nil {
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"field or string"}, pos)
	}

	return query.FieldSelector(field), nil
}

func (p *Parser) parseFrom() (string, error) {
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.FROM {
		return "", newParseError(scanner.Tokstr(tok, lit), []string{"FROM"}, pos)
	}

	// Parse table name
	return p.ParseIdent()
}

func (p *Parser) parseOrderBy() (query.FieldSelector, scanner.Token, error) {
	// parse ORDER token
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.ORDER {
		p.Unscan()
		return nil, 0, nil
	}

	// parse BY token
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.BY {
		return nil, 0, newParseError(scanner.Tokstr(tok, lit), []string{"BY"}, pos)
	}

	// parse field reference
	ref, err := p.ParseFieldRef()
	if err != nil {
		return nil, 0, err
	}

	// parse optional ASC or DESC
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.ASC || tok == scanner.DESC {
		return query.FieldSelector(ref), tok, nil
	}
	p.Unscan()

	return query.FieldSelector(ref), 0, nil
}

func (p *Parser) parseLimit() (query.Expr, error) {
	// parse LIMIT token
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.LIMIT {
		p.Unscan()
		return nil, nil
	}

	return p.ParseExpr()
}

func (p *Parser) parseOffset() (query.Expr, error) {
	// parse OFFSET token
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.OFFSET {
		p.Unscan()
		return nil, nil
	}

	return p.ParseExpr()
}
