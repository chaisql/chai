package parser

import (
	"github.com/genjidb/genji/sql/query"
	"github.com/genjidb/genji/sql/scanner"
)

// parseAlterStatement parses a Alter query string and returns a Statement AST object.
// This function assumes the ALTER token has already been consumed.
func (p *Parser) parseAlterStatement() (query.AlterStmt, error) {
	var stmt query.AlterStmt
	var err error

	// Parse "TABLE".
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.TABLE {
		return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"TABLE"}, pos)
	}

	// Parse table name.
	stmt.TableName, err = p.parseIdent()
	if err != nil {
		return stmt, err
	}

	// Parse "RENAME".
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.RENAME {
		return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"RENAME"}, pos)
	}

	// Parse "TO".
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.TO {
		return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"TO"}, pos)
	}

	// Parse new table name.
	stmt.NewName, err = p.parseIdent()
	if err != nil {
		return stmt, err
	}

	return stmt, nil
}
