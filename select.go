package genji

import (
	"github.com/asdine/genji/internal/scanner"
)

// parseSelectStatement parses a select string and returns a Statement AST object.
// This function assumes the SELECT token has already been consumed.
func (p *parser) parseSelectStatement() (selectStmt, error) {
	var stmt selectStmt
	var err error

	// Parse field list or wildcard
	stmt.selectors, err = p.parseResultFields()
	if err != nil {
		return stmt, err
	}

	// Parse "FROM".
	stmt.tableName, err = p.parseFrom()
	if err != nil {
		return stmt, err
	}

	// Parse condition: "WHERE EXPR".
	stmt.whereExpr, err = p.parseCondition()
	if err != nil {
		return stmt, err
	}

	stmt.limitExpr, err = p.parseLimit()
	if err != nil {
		return stmt, err
	}

	stmt.offsetExpr, err = p.parseOffset()
	if err != nil {
		return stmt, err
	}

	stmt.stat = p.stat
	return stmt, nil
}

// parseResultFields parses the list of result fields.
func (p *parser) parseResultFields() ([]resultField, error) {
	// Parse first (required) result field.
	rf, err := p.parseResultField()
	if err != nil {
		return nil, err
	}
	rfields := []resultField{rf}

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
func (p *parser) parseResultField() (resultField, error) {
	// Check if the * token exists.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.MUL {
		return wildcard{}, nil
	}
	p.Unscan()

	// Check if it's the key() function
	tok, pos, lit := p.ScanIgnoreWhitespace()
	if tok == scanner.KEY {
		if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.LPAREN {
			if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.RPAREN {
				return nil, newParseError(scanner.Tokstr(tok, lit), []string{")"}, pos)
			}

			return keyFunc{}, nil
		}
	}
	p.Unscan()

	// Check if it's an identifier
	ident, err := p.ParseIdent()
	if err != nil {
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"ident or string"}, pos)
	}

	return fieldSelector(ident), nil
}

func (p *parser) parseFrom() (string, error) {
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.FROM {
		return "", newParseError(scanner.Tokstr(tok, lit), []string{"FROM"}, pos)
	}

	// Parse table name
	return p.ParseIdent()
}

func (p *parser) parseLimit() (expr, error) {
	// parse LIMIT token
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.LIMIT {
		p.Unscan()
		return nil, nil
	}

	return p.ParseExpr()
}

func (p *parser) parseOffset() (expr, error) {
	// parse OFFSET token
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.OFFSET {
		p.Unscan()
		return nil, nil
	}

	return p.ParseExpr()
}
