package parser

import (
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/scanner"
)

// parseReindexStatement parses a reindex statement.
// This function assumes the REINDEX token has already been consumed.
func (p *Parser) parseReIndexStatement() (statement.Statement, error) {
	var stmt statement.ReIndexStmt
	var err error

	tok, _, lit := p.ScanIgnoreWhitespace()
	if tok == scanner.IDENT {
		stmt.TableOrIndexName = lit
	} else {
		p.Unscan()
	}
	return stmt, err
}
