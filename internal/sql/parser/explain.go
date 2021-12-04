package parser

import (
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/scanner"
)

// parseExplainStatement parses any statement and returns an ExplainStmt object.
// This function assumes the EXPLAIN token has already been consumed.
func (p *Parser) parseExplainStatement() (statement.Statement, error) {
	// Parse "EXPLAIN".
	if err := p.parseTokens(scanner.EXPLAIN); err != nil {
		return nil, err
	}

	// ensure we don't have multiple EXPLAIN keywords
	tok, pos, lit := p.ScanIgnoreWhitespace()
	if tok != scanner.SELECT && tok != scanner.UPDATE && tok != scanner.DELETE && tok != scanner.INSERT {
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"INSERT", "SELECT", "UPDATE", "DELETE"}, pos)
	}
	p.Unscan()

	innerStmt, err := p.ParseStatement()
	if err != nil {
		return nil, err
	}

	return &statement.ExplainStmt{Statement: innerStmt.(statement.Preparer)}, nil
}
