package parser

import (
	"github.com/cockroachdb/errors"

	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/scanner"
)

func (p *Parser) parseAlterTableRenameStatement(tableName string) (_ statement.AlterTableRenameStmt, err error) {
	var stmt statement.AlterTableRenameStmt
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

func (p *Parser) parseAlterTableAddFieldStatement(tableName string) (*statement.AlterTableAddFieldStmt, error) {
	var stmt statement.AlterTableAddFieldStmt
	stmt.TableName = tableName

	// Parse "FIELD".
	if err := p.parseTokens(scanner.COLUMN); err != nil {
		return nil, err
	}

	// Parse new field definition.
	var err error
	stmt.FieldConstraint, stmt.TableConstraints, err = p.parseFieldDefinition(nil)
	if err != nil {
		return nil, err
	}

	if stmt.FieldConstraint.IsEmpty() {
		return nil, &ParseError{Message: "cannot add a field with no constraint"}
	}

	return &stmt, nil
}

// parseAlterStatement parses a Alter query string and returns a Statement AST object.
func (p *Parser) parseAlterStatement() (statement.Statement, error) {
	var err error

	// Parse "TABLE".
	if err := p.parseTokens(scanner.ALTER, scanner.TABLE); err != nil {
		return nil, err
	}

	// Parse table name.
	tableName, err := p.parseIdent()
	if err != nil {
		pErr := errors.Unwrap(err).(*ParseError)
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
