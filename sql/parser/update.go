package parser

import (
	"github.com/genjidb/genji/sql/query"
	"github.com/genjidb/genji/sql/query/expr"
	"github.com/genjidb/genji/sql/scanner"
	"strings"
)

// parseUpdateStatement parses a update string and returns a Statement AST object.
// This function assumes the UPDATE token has already been consumed.
func (p *Parser) parseUpdateStatement() (query.UpdateStmt, error) {
	var stmt query.UpdateStmt
	var err error

	// Parse table name
	stmt.TableName, err = p.parseIdent()
	if err != nil {
		return stmt, err
	}

	// Parse clause: SET or UNSET.
	tok, pos, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.SET:
		stmt.SetPairs, err = p.parseSetClause()
	case scanner.UNSET:
		stmt.UnsetFields, err = p.parseUnsetClause()
	default:
		err = newParseError(scanner.Tokstr(tok, lit), []string{"SET", "UNSET"}, pos)
	}
	if err != nil {
		return stmt, err
	}

	// Parse condition: "WHERE EXPR".
	stmt.WhereExpr, err = p.parseCondition()
	if err != nil {
		return stmt, err
	}

	return stmt, nil
}

// parseSetClause parses the "SET" clause of the query.
func (p *Parser) parseSetClause() (map[string]expr.Expr, error) {
	setPairs := make(map[string]expr.Expr)
	firstPair := true
	for {
		if !firstPair {
			// Scan for a comma.
			tok, _, _ := p.ScanIgnoreWhitespace()
			if tok != scanner.COMMA {
				p.Unscan()
				break
			}
		}

		// Scan the identifier for the field name.
		tok, pos, lit := p.ScanIgnoreWhitespace()
		if tok != scanner.IDENT && tok != scanner.STRING {
			return nil, newParseError(scanner.Tokstr(tok, lit), []string{"identifier"}, pos)
		}
		p.Unscan()
		ref, err := p.parseFieldRef()
		if err != nil {
			return nil, newParseError(scanner.Tokstr(tok, lit), []string{"identifier"}, pos)
		}

		// Scan the eq sign
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.EQ {
			return nil, newParseError(scanner.Tokstr(tok, lit), []string{"="}, pos)
		}

		// Scan the expr for the value.
		expr, _, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}
		req := strings.Join(ref, ".")
		setPairs[req] = expr
		firstPair = false
	}
	return setPairs, nil
}

func (p *Parser) parseUnsetClause() ([]string, error) {
	var fields []string

	firstField := true
	for {
		if !firstField {
			// Scan for a comma.
			tok, _, _ := p.ScanIgnoreWhitespace()
			if tok != scanner.COMMA {
				p.Unscan()
				break
			}
		}

		// Scan the identifier for the field to unset.
		tok, pos, lit := p.ScanIgnoreWhitespace()
		if tok != scanner.IDENT {
			return nil, newParseError(scanner.Tokstr(tok, lit), []string{"identifier"}, pos)
		}
		fields = append(fields, lit)

		firstField = false
	}
	return fields, nil
}
