package parser

import (
	"github.com/genjidb/genji/internal/query"
	"github.com/genjidb/genji/internal/sql/scanner"
)

func (p *Parser) parseAlterTableRenameStatement(tableName string) (_ query.AlterStmt, err error) {
	var stmt query.AlterStmt
	stmt.TableName = tableName

	// Parse "TO".
	if err := p.parseTokens(scanner.TO); err != nil {
		return stmt, err
	}

	// Parse new table name.
	stmt.NewTableName, err = p.parseIdent()
	if err != nil {
		return stmt, err
	}

	return stmt, nil
}

func (p *Parser) parseAlterTableAddFieldStatement(tableName string) (_ query.AlterTableAddField, err error) {
	var stmt query.AlterTableAddField
	stmt.TableName = tableName

	// Parse "FIELD".
	if err := p.parseTokens(scanner.FIELD); err != nil {
		return stmt, err
	}

	// Parse new field definition.
	err = p.parseFieldDefinition(&stmt.Constraint)
	if err != nil {
		return stmt, err
	}

	if stmt.Constraint.IsPrimaryKey {
		return stmt, &ParseError{Message: "cannot add a PRIMARY KEY constraint"}
	}

	return stmt, nil
}

// parseAlterStatement parses a Alter query string and returns a Statement AST object.
// This function assumes the ALTER token has already been consumed.
func (p *Parser) parseAlterStatement() (query.Statement, error) {
	var err error

	// Parse "TABLE".
	if err := p.parseTokens(scanner.TABLE); err != nil {
		return nil, err
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
