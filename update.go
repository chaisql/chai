package genji

import (
	"github.com/asdine/genji/query"
	"github.com/asdine/genji/query/expr"
	"github.com/asdine/genji/sql/scanner"
)

// parseUpdateStatement parses a update string and returns a query.Statement AST object.
// This function assumes the UPDATE token has already been consumed.
func (p *Parser) parseUpdateStatement() (query.UpdateStmt, error) {
	// Parse table name
	tableName, err := p.ParseIdent()
	if err != nil {
		return query.Update(""), err
	}

	stmt := query.Update(tableName)

	// Parse assignment: "SET field = EXPR".
	pairs, err := p.parseSetClause()
	if err != nil {
		return stmt, err
	}
	for k, v := range pairs {
		stmt = stmt.Set(k, v)
	}

	// Parse condition: "WHERE EXPR".
	where, err := p.parseCondition()
	if err != nil {
		return stmt, err
	}
	stmt = stmt.Where(where)

	return stmt, nil
}

// parseSetClause parses the "SET" clause of the query.
func (p *Parser) parseSetClause() (map[string]expr.Expr, error) {
	// Check if the SET token exists.
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.SET {
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"SET"}, pos)
	}

	pairs := make(map[string]expr.Expr)

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
		if tok != scanner.IDENT {
			return nil, newParseError(scanner.Tokstr(tok, lit), []string{"identifier"}, pos)
		}

		// Scan the eq sign
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.EQ {
			return nil, newParseError(scanner.Tokstr(tok, lit), []string{"="}, pos)
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
