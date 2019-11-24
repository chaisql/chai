package parser

import (
	"github.com/asdine/genji/internal/scanner"
	"github.com/asdine/genji/query"
)

// parseDropStatement parses a drop string and returns a Statement AST object.
// This function assumes the DROP token has already been consumed.
func (p *parser) parseDropStatement() (query.Statement, error) {
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
func (p *parser) parseDropTableStatement() (query.DropTableStmt, error) {
	var stmt query.DropTableStmt
	var err error

	// Parse "IF"
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.IF {
		// Parse "EXISTS"
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.EXISTS {
			return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"EXISTS"}, pos)
		}
		stmt.IfExists = true
	} else {
		p.Unscan()
	}

	// Parse table name
	stmt.TableName, err = p.ParseIdent()
	if err != nil {
		return stmt, err
	}

	return stmt, nil
}

// parseDropIndexStatement parses a drop index string and returns a Statement AST object.
// This function assumes the DROP INDEX tokens have already been consumed.
func (p *parser) parseDropIndexStatement() (query.DropIndexStmt, error) {
	var stmt query.DropIndexStmt
	var err error

	// Parse "IF"
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.IF {
		// Parse "EXISTS"
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.EXISTS {
			return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"EXISTS"}, pos)
		}
		stmt.IfExists = true
	} else {
		p.Unscan()
	}

	// Parse index name
	stmt.IndexName, err = p.ParseIdent()
	if err != nil {
		return stmt, err
	}

	return stmt, nil
}
