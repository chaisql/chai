package parser

import (
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/scanner"
)

// parseReindexStatement parses a reindex statement.
func (p *Parser) parseReIndexStatement() (statement.Statement, error) {
	stmt := statement.NewReIndexStatement()

	// Parse "REINDEX".
	if err := p.parseTokens(scanner.REINDEX); err != nil {
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
