package parser

import (
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/scanner"
	"github.com/cockroachdb/errors"
)

// parseUpdateStatement parses a update string and returns a Statement AST row.
func (p *Parser) parseUpdateStatement() (*statement.UpdateStmt, error) {
	var stmt statement.UpdateStmt
	var err error

	// Parse "UPDATE".
	if err := p.ParseTokens(scanner.UPDATE); err != nil {
		return nil, err
	}

	// Parse table name
	stmt.TableName, err = p.parseIdent()
	if err != nil {
		pErr := errors.Unwrap(err).(*ParseError)
		pErr.Expected = []string{"table_name"}
		return nil, pErr
	}

	// Parse clause: SET.
	tok, pos, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.SET:
		stmt.SetPairs, err = p.parseSetClause()
	default:
		err = newParseError(scanner.Tokstr(tok, lit), []string{"SET"}, pos)
	}
	if err != nil {
		return nil, err
	}

	// Parse condition: "WHERE EXPR".
	stmt.WhereExpr, err = p.parseCondition()
	if err != nil {
		return nil, err
	}

	return &stmt, nil
}

// parseSetClause parses the "SET" clause of the query.
func (p *Parser) parseSetClause() ([]statement.UpdateSetPair, error) {
	var pairs []statement.UpdateSetPair

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

		// Scan the identifier for the col name.
		col, err := p.parseColumn()
		if err != nil {
			pErr := errors.Unwrap(err).(*ParseError)
			pErr.Expected = []string{"path"}
			return nil, pErr
		}

		// Scan the eq sign
		if err := p.ParseTokens(scanner.EQ); err != nil {
			return nil, err
		}

		// Scan the expr for the value.
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, statement.UpdateSetPair{Column: col, E: expr})

		firstPair = false
	}

	return pairs, nil
}
