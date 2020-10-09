package parser

import (
	"github.com/genjidb/genji/sql/query"
	"github.com/genjidb/genji/sql/scanner"
)

func (p *Parser) parseAlterTableRenameStatement(tableName string) (_ query.AlterStmt, err error) {
	var stmt query.AlterStmt
	stmt.TableName = tableName

	// Parse "TO".
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.TO {
		return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"TO"}, pos)
	}

	// Parse new table name.
	stmt.NewTableName, err = p.parseIdent()
	if err != nil {
		return stmt, err
	}

	return stmt, nil
}

func (p *Parser) parseAlterTableAddFieldStatement(tableName string) (_ query.AlterTableAddColumn, err error) {
	var stmt query.AlterTableAddColumn
	stmt.TableName = tableName

	// Parse "COLUMN" or "FIELD".
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.COLUMN && tok != scanner.FIELD {
		return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"COLUMN", "FIELD"}, pos)
	}

	// Parse new field definition.
	err = p.parseField(&stmt.Constraint)
	if err != nil {
		return stmt, err
	}

	return stmt, nil
}

// parseAlterStatement parses a Alter query string and returns a Statement AST object.
// This function assumes the ALTER token has already been consumed.
func (p *Parser) parseAlterStatement() (query.Statement, error) {
	var err error

	// Parse "TABLE".
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.TABLE {
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"TABLE"}, pos)
	}

	// Parse table name.
	tableName, err := p.parseIdent()
	if err != nil {
		pErr := err.(*ParseError)
		pErr.Expected = []string{"table_name"}
		return nil, pErr
	}

	tok, pos, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.RENAME:
		return p.parseAlterTableRenameStatement(tableName)
	case scanner.ADD_KEYWORD:
		return p.parseAlterTableAddFieldStatement(tableName)
	}

	return nil, newParseError(scanner.Tokstr(tok, lit), []string{"ADD", "RENAME"}, pos)
}
