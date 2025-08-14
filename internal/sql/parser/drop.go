package parser

import (
	"github.com/cockroachdb/errors"

	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/scanner"
)

// parseDropStatement parses a drop string and returns a Statement AST row.
func (p *Parser) parseDropStatement() (statement.Statement, error) {
	// Parse "DROP".
	if err := p.ParseTokens(scanner.DROP); err != nil {
		return nil, err
	}

	tok, pos, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.TABLE:
		return p.parseDropTableStatement()
	case scanner.INDEX:
		return p.parseDropIndexStatement()
	case scanner.SEQUENCE:
		return p.parseDropSequenceStatement()
	}

	return nil, newParseError(scanner.Tokstr(tok, lit), []string{"TABLE", "INDEX", "SEQUENCE"}, pos)
}

// parseDropTableStatement parses a drop table string and returns a Statement AST row.
// This function assumes the DROP TABLE tokens have already been consumed.
func (p *Parser) parseDropTableStatement() (*statement.DropTableStmt, error) {
	var stmt statement.DropTableStmt
	var err error

	stmt.IfExists, err = p.parseOptional(scanner.IF, scanner.EXISTS)
	if err != nil {
		return nil, err
	}

	// Parse table name
	stmt.TableName, err = p.parseIdent()
	if err != nil {
		pErr := errors.Unwrap(err).(*ParseError)
		pErr.Expected = []string{"table_name"}
		return nil, pErr
	}

	return &stmt, nil
}

// parseDropIndexStatement parses a drop index string and returns a Statement AST row.
// This function assumes the DROP INDEX tokens have already been consumed.
func (p *Parser) parseDropIndexStatement() (*statement.DropIndexStmt, error) {
	var stmt statement.DropIndexStmt
	var err error

	stmt.IfExists, err = p.parseOptional(scanner.IF, scanner.EXISTS)
	if err != nil {
		return nil, err
	}

	// Parse index name
	stmt.IndexName, err = p.parseIdent()
	if err != nil {
		pErr := errors.Unwrap(err).(*ParseError)
		pErr.Expected = []string{"index_name"}
		return nil, pErr
	}

	return &stmt, nil
}

// parseDropSequenceStatement parses a drop sequence string and returns a Statement AST row.
// This function assumes the DROP SEQUENCE tokens have already been consumed.
func (p *Parser) parseDropSequenceStatement() (*statement.DropSequenceStmt, error) {
	var stmt statement.DropSequenceStmt
	var err error

	stmt.IfExists, err = p.parseOptional(scanner.IF, scanner.EXISTS)
	if err != nil {
		return nil, err
	}

	// Parse sequence name
	stmt.SequenceName, err = p.parseIdent()
	if err != nil {
		pErr := errors.Unwrap(err).(*ParseError)
		pErr.Expected = []string{"sequence_name"}
		return nil, pErr
	}

	return &stmt, nil
}
