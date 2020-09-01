package parser

import (
	"github.com/genjidb/genji/sql/query"
	"github.com/genjidb/genji/sql/scanner"
)

// parseBeginStatement parses a BEGIN statement.
// This function assumes the BEGIN token has already been consumed.
func (p *Parser) parseBeginStatement() (query.Statement, error) {
	// parse optional TRANSCACTION token
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.TRANSACTION {
		p.Unscan()
	}

	return query.BeginStmt{Writable: true}, nil
}

// parseRollbackStatement parses a ROLLBACK statement.
// This function assumes the ROLLBACK token has already been consumed.
func (p *Parser) parseRollbackStatement() (query.Statement, error) {
	// parse optional TRANSCACTION token
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.TRANSACTION {
		p.Unscan()
	}

	return query.RollbackStmt{}, nil
}

// parseCommitStatement parses a COMMIT statement.
// This function assumes the COMMIT token has already been consumed.
func (p *Parser) parseCommitStatement() (query.Statement, error) {
	// parse optional TRANSCACTION token
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.TRANSACTION {
		p.Unscan()
	}

	return query.CommitStmt{}, nil
}
