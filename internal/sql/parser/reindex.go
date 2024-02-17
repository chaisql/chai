package parser

import (
	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/scanner"
)

// parseReindexStatement parses a reindex statement.
func (p *Parser) parseReIndexStatement() (statement.Statement, error) {
	stmt := statement.NewReIndexStatement()

	// Parse "REINDEX".
	if err := p.ParseTokens(scanner.REINDEX); err != nil {
		return nil, err
	}

	tok, _, lit := p.ScanIgnoreWhitespace()
	if tok == scanner.IDENT {
		stmt.TableOrIndexName = lit
	} else {
		p.Unscan()
	}
	return stmt, nil
}
