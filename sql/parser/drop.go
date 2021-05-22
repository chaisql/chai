package parser

import (
	"github.com/genjidb/genji/internal/query"
	"github.com/genjidb/genji/sql/scanner"
)

// parseDropStatement parses a drop string and returns a Statement AST object.
// This function assumes the DROP token has already been consumed.
func (p *Parser) parseDropStatement() (query.Statement, error) {
	tok, pos, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.TABLE:
		return p.parseDropTableStatement()
	case scanner.INDEX:
		return p.parseDropIndexStatement()
	}

	return nil, newParseError(scanner.Tokstr(tok, lit), []string{"TABLE", "INDEX"}, pos)
}

// parseDropTableStatement parses a drop table string and returns a Statement AST object.
// This function assumes the DROP TABLE tokens have already been consumed.
func (p *Parser) parseDropTableStatement() (query.DropTableStmt, error) {
	var stmt query.DropTableStmt
	var err error

	stmt.IfExists, err = p.parseOptional(scanner.IF, scanner.EXISTS)
	if err != nil {
		return stmt, err
	}

	// Parse table name
	stmt.TableName, err = p.parseIdent()
	if err != nil {
		pErr := err.(*ParseError)
		pErr.Expected = []string{"table_name"}
		return stmt, pErr
	}

	return stmt, nil
}

// parseDropIndexStatement parses a drop index string and returns a Statement AST object.
// This function assumes the DROP INDEX tokens have already been consumed.
func (p *Parser) parseDropIndexStatement() (query.DropIndexStmt, error) {
	var stmt query.DropIndexStmt
	var err error

	stmt.IfExists, err = p.parseOptional(scanner.IF, scanner.EXISTS)
	if err != nil {
		return stmt, err
	}

	// Parse index name
	stmt.IndexName, err = p.parseIdent()
	if err != nil {
		pErr := err.(*ParseError)
		pErr.Expected = []string{"index_name"}
		return stmt, pErr
	}

	return stmt, nil
}
