package parser

import (
	"github.com/cockroachdb/errors"

	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/scanner"
)

func (p *Parser) parseAlterTableRenameStatement(tableName string) (_ statement.AlterStmt, err error) {
	var stmt statement.AlterStmt
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

func (p *Parser) parseAlterTableAddFieldStatement(tableName string) (_ statement.AlterTableAddField, err error) {
	var stmt statement.AlterTableAddField
	stmt.Info.TableName = tableName

	// Parse "FIELD".
	if err := p.parseTokens(scanner.FIELD); err != nil {
		return stmt, err
	}

	// Parse new field definition.
	fc, tcs, err := p.parseFieldDefinition(nil)
	if err != nil {
		return stmt, err
	}

	if fc.IsEmpty() {
		return stmt, &ParseError{Message: "cannot add a field with no constraint"}
	}

	err = stmt.Info.AddFieldConstraint(fc)
	if err != nil {
		return stmt, err
	}

	for _, tc := range tcs {
		err = stmt.Info.AddTableConstraint(tc)
		if err != nil {
			return stmt, err
		}
	}

	if stmt.Info.GetPrimaryKey() != nil {
		return stmt, &ParseError{Message: "cannot add a PRIMARY KEY constraint"}
	}

	return stmt, nil
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
