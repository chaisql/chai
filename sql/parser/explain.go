package parser

import (
	"github.com/genjidb/genji/internal/query"
	"github.com/genjidb/genji/sql/scanner"
)

// parseExplainStatement parses any statement and returns an ExplainStmt object.
// This function assumes the EXPLAIN token has already been consumed.
func (p *Parser) parseExplainStatement() (query.Statement, error) {
	// ensure we don't have multiple EXPLAIN keywords
	tok, pos, lit := p.ScanIgnoreWhitespace()
	if tok == scanner.EXPLAIN {
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"SELECT", "UPDATE", "DELETE"}, pos)
	}
	p.Unscan()

	innerStmt, err := p.ParseStatement()
	if err != nil {
		return nil, err
	}

	return &query.ExplainStmt{Statement: innerStmt}, nil
}
