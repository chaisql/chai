package parser

import (
	"github.com/genjidb/genji/internal/query"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/scanner"
)

// parseBeginStatement parses a BEGIN statement.
func (p *Parser) parseBeginStatement() (statement.Statement, error) {
	// Parse "BEGIN".
	if err := p.parseTokens(scanner.BEGIN); err != nil {
		return nil, err
	}

	// parse optional TRANSACTION token
	_, _ = p.parseOptional(scanner.TRANSACTION)

	// parse optional READ token
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.READ {
		p.Unscan()
		return query.BeginStmt{Writable: true}, nil
	}

	// parse ONLY token
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.ONLY {
		return query.BeginStmt{Writable: false}, nil
	}

	p.Unscan()

	// parse WRITE token
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.WRITE {
		return query.BeginStmt{}, newParseError(scanner.Tokstr(tok, lit), []string{"ONLY", "WRITE"}, pos)
	}

	return query.BeginStmt{Writable: true}, nil
}

// parseRollbackStatement parses a ROLLBACK statement.
func (p *Parser) parseRollbackStatement() (statement.Statement, error) {
	// Parse "ROLLBACK".
	if err := p.parseTokens(scanner.ROLLBACK); err != nil {
		return nil, err
	}

	// parse optional TRANSACTION token
	_, _ = p.parseOptional(scanner.TRANSACTION)

	return query.RollbackStmt{}, nil
}

// parseCommitStatement parses a COMMIT statement.
func (p *Parser) parseCommitStatement() (statement.Statement, error) {
	// Parse "COMMIT".
	if err := p.parseTokens(scanner.COMMIT); err != nil {
		return nil, err
	}
	// parse optional TRANSACTION token
	_, _ = p.parseOptional(scanner.TRANSACTION)

	return query.CommitStmt{}, nil
}
