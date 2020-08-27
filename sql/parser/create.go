package parser

import (
	"fmt"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/sql/query"
	"github.com/genjidb/genji/sql/scanner"
)

// parseCreateStatement parses a create string and returns a Statement AST object.
// This function assumes the CREATE token has already been consumed.
func (p *Parser) parseCreateStatement() (query.Statement, error) {
	tok, pos, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.TABLE:
		return p.parseCreateTableStatement()
	case scanner.UNIQUE:
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.INDEX {
			return nil, newParseError(scanner.Tokstr(tok, lit), []string{"INDEX"}, pos)
		}

		return p.parseCreateIndexStatement(true)
	case scanner.INDEX:
		return p.parseCreateIndexStatement(false)
	}

	return nil, newParseError(scanner.Tokstr(tok, lit), []string{"TABLE", "INDEX"}, pos)
}

// parseCreateTableStatement parses a create table string and returns a Statement AST object.
// This function assumes the CREATE TABLE tokens have already been consumed.
func (p *Parser) parseCreateTableStatement() (query.CreateTableStmt, error) {
	var stmt query.CreateTableStmt
	var err error

	// Parse IF NOT EXISTS
	stmt.IfNotExists, err = p.parseIfNotExists()
	if err != nil {
		return stmt, err
	}

	// Parse table name
	stmt.TableName, err = p.parseIdent()
	if err != nil {
		return stmt, err
	}

	// parse path constraints
	err = p.parseFieldConstraints(&stmt.Info)
	if err != nil {
		return stmt, err
	}

	return stmt, nil
}

func (p *Parser) parseIfNotExists() (bool, error) {
	// Parse "IF"
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.IF {
		p.Unscan()
		return false, nil
	}

	// Parse "NOT"
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.NOT {
		return false, newParseError(scanner.Tokstr(tok, lit), []string{"NOT", "EXISTS"}, pos)
	}

	// Parse "EXISTS"
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.EXISTS {
		return false, newParseError(scanner.Tokstr(tok, lit), []string{"EXISTS"}, pos)
	}

	return true, nil
}

func (p *Parser) parseFieldConstraints(info *database.TableInfo) error {
	// Parse ( token.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.LPAREN {
		p.Unscan()
		return nil
	}

	var err error

	// Parse constraints.
	for {
		var fc database.FieldConstraint

		fc.Path, err = p.parsePath()
		if err != nil {
			p.Unscan()
			break
		}

		fc.Type = p.parseType()

		err = p.parseFieldConstraint(&fc)
		if err != nil {
			return err
		}

		info.FieldConstraints = append(info.FieldConstraints, fc)

		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			break
		}
	}

	// Parse required ) token.
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.RPAREN {
		return newParseError(scanner.Tokstr(tok, lit), []string{")"}, pos)
	}

	// ensure only one primary key
	var pkCount int
	for _, fc := range info.FieldConstraints {
		if fc.IsPrimaryKey {
			pkCount++
		}
	}
	if pkCount > 1 {
		return &ParseError{Message: fmt.Sprintf("only one primary key is allowed, got %d", pkCount)}
	}

	return nil
}

func (p *Parser) parseFieldConstraint(fc *database.FieldConstraint) error {
	for {
		tok, pos, lit := p.ScanIgnoreWhitespace()
		switch tok {
		case scanner.PRIMARY:
			// Parse "KEY"
			if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.KEY {
				return newParseError(scanner.Tokstr(tok, lit), []string{"KEY"}, pos)
			}

			// if it's already a primary key we return an error
			if fc.IsPrimaryKey {
				return newParseError(scanner.Tokstr(tok, lit), []string{"CONSTRAINT", ")"}, pos)
			}

			fc.IsPrimaryKey = true
		case scanner.NOT:
			// Parse "NULL"
			if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.NULL {
				return newParseError(scanner.Tokstr(tok, lit), []string{"NULL"}, pos)
			}

			// if it's already not null we return an error
			if fc.IsNotNull {
				return newParseError(scanner.Tokstr(tok, lit), []string{"CONSTRAINT", ")"}, pos)
			}

			fc.IsNotNull = true
		default:
			p.Unscan()
			return nil
		}
	}
}

// parseCreateIndexStatement parses a create index string and returns a Statement AST object.
// This function assumes the CREATE INDEX or CREATE UNIQUE INDEX tokens have already been consumed.
func (p *Parser) parseCreateIndexStatement(unique bool) (query.CreateIndexStmt, error) {
	var err error
	stmt := query.CreateIndexStmt{
		Unique: unique,
	}

	// Parse "IF"
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.IF {
		// Parse "NOT"
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.NOT {
			return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"NOT", "EXISTS"}, pos)
		}

		// Parse "EXISTS"
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.EXISTS {
			return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"EXISTS"}, pos)
		}

		stmt.IfNotExists = true
	} else {
		p.Unscan()
	}

	// Parse index name
	stmt.IndexName, err = p.parseIdent()
	if err != nil {
		return stmt, err
	}

	// Parse "ON"
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.ON {
		return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"ON"}, pos)
	}

	// Parse table name
	stmt.TableName, err = p.parseIdent()
	if err != nil {
		return stmt, err
	}

	paths, err := p.parsePathList()
	if err != nil {
		return stmt, err
	}
	if len(paths) == 0 {
		tok, pos, lit := p.ScanIgnoreWhitespace()
		return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"("}, pos)
	}

	if len(paths) != 1 {
		return stmt, &ParseError{Message: "indexes on more than one path are not supported"}
	}

	stmt.Path = paths[0]

	return stmt, nil
}
