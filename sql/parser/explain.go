package parser

import (
	"github.com/genjidb/genji/sql/planner"
	"github.com/genjidb/genji/sql/query"
	"github.com/genjidb/genji/sql/scanner"
)

// parseDropStatement parses a drop string and returns a Statement AST object.
// This function assumes the DROP token has already been consumed.
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

	return &planner.ExplainStmt{Statement: innerStmt}, nil
}
