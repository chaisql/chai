package parser

import (
	"github.com/cockroachdb/errors"

	"github.com/chaisql/chai/internal/query/statement"
	"github.com/chaisql/chai/internal/sql/scanner"
)

func (p *Parser) parseAlterTableRenameStatement(tableName string) (_ *statement.AlterTableRenameStmt, err error) {
	var stmt statement.AlterTableRenameStmt
	stmt.TableName = tableName

	// Parse "TO".
	if err := p.ParseTokens(scanner.TO); err != nil {
		return nil, err
	}

	// Parse new table name.
	stmt.NewTableName, err = p.parseIdent()
	if err != nil {
		return nil, err
	}

	return &stmt, nil
}

func (p *Parser) parseAlterTableAddColumnStatement(tableName string) (*statement.AlterTableAddColumnStmt, error) {
	var stmt statement.AlterTableAddColumnStmt
	stmt.TableName = tableName

	// Parse "COLUMN".
	if err := p.ParseTokens(scanner.COLUMN); err != nil {
		return nil, err
	}

	// Parse new column definition.
	var err error
	stmt.ColumnConstraint, stmt.TableConstraints, err = p.parseColumnDefinition()
	if err != nil {
		return nil, err
	}

	if stmt.ColumnConstraint.IsEmpty() {
		return nil, &ParseError{Message: "cannot add a column with no constraint"}
	}

	return &stmt, nil
}

// parseAlterStatement parses a Alter query string and returns a Statement AST row.
func (p *Parser) parseAlterStatement() (statement.Statement, error) {
	var err error

	// Parse "TABLE".
	if err := p.ParseTokens(scanner.ALTER, scanner.TABLE); err != nil {
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
		return p.parseAlterTableAddColumnStatement(tableName)
	}

	return nil, newParseError(scanner.Tokstr(tok, lit), []string{"ADD", "RENAME"}, pos)
}
