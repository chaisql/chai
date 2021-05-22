package parser

import (
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/query"
	"github.com/genjidb/genji/internal/stringutil"
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
	stmt.IfNotExists, err = p.parseOptional(scanner.IF, scanner.NOT, scanner.EXISTS)
	if err != nil {
		return stmt, err
	}

	// Parse table name
	stmt.Info.TableName, err = p.parseIdent()
	if err != nil {
		return stmt, err
	}

	// parse field constraints
	err = p.parseConstraints(&stmt)
	return stmt, err
}

func (p *Parser) parseFieldDefinition(fc *database.FieldConstraint) (err error) {
	fc.Path, err = p.parsePath()
	if err != nil {
		return err
	}

	fc.Type, err = p.parseType()
	if err != nil {
		p.Unscan()
	}

	err = p.parseFieldConstraint(fc)
	if err != nil {
		return err
	}

	if fc.Type.IsAny() && fc.DefaultValue.Type.IsAny() && !fc.IsNotNull && !fc.IsPrimaryKey && !fc.IsUnique {
		tok, pos, lit := p.ScanIgnoreWhitespace()
		return newParseError(scanner.Tokstr(tok, lit), []string{"CONSTRAINT", "TYPE"}, pos)
	}

	return nil
}

func (p *Parser) parseConstraints(stmt *query.CreateTableStmt) error {
	// Parse ( token.
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.LPAREN {
		p.Unscan()
		return nil
	}

	// if set to true, the parser must no longer
	// expect field definitions, but only table constraints.
	var parsingTableConstraints bool

	// Parse constraints.
	for {
		// we start by checking if it is a table constraint,
		// as it's easier to determine
		ok, err := p.parseTableConstraint(stmt)
		if err != nil {
			return err
		}

		// no table constraint found
		if !ok && parsingTableConstraints {
			tok, pos, lit := p.ScanIgnoreWhitespace()
			return newParseError(scanner.Tokstr(tok, lit), []string{"CONSTRAINT", ")"}, pos)
		}

		// only PRIMARY KEY(path) is currently supported.
		if ok {
			parsingTableConstraints = true
		}

		// if set to false, we are still parsing field definitions
		if !parsingTableConstraints {
			var fc database.FieldConstraint

			err = p.parseFieldDefinition(&fc)
			if err != nil {
				return err
			}

			stmt.Info.FieldConstraints = append(stmt.Info.FieldConstraints, &fc)
		}

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
	var pkFound bool
	for _, fc := range stmt.Info.FieldConstraints {
		if fc.IsPrimaryKey {
			if pkFound {
				return stringutil.Errorf("table %q has more than one primary key", stmt.Info.TableName)
			}

			pkFound = true
		}
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
		case scanner.DEFAULT:
			// Parse default value expression.
			e, err := p.parseUnaryExpr()
			if err != nil {
				return err
			}

			d, err := e.Eval(&expr.Environment{})
			if err != nil {
				return err
			}

			// if it has already a default value we return an error
			if fc.HasDefaultValue() {
				return newParseError(scanner.Tokstr(tok, lit), []string{"CONSTRAINT", ")"}, pos)
			}

			fc.DefaultValue = d
		case scanner.UNIQUE:
			// if it's already unique we return an error
			if fc.IsUnique {
				return newParseError(scanner.Tokstr(tok, lit), []string{"CONSTRAINT", ")"}, pos)
			}

			fc.IsUnique = true
		default:
			p.Unscan()
			return nil
		}
	}
}

func (p *Parser) parseTableConstraint(stmt *query.CreateTableStmt) (bool, error) {
	var err error

	tok, _, _ := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.PRIMARY:
		// Parse "KEY ("
		err = p.parseTokens(scanner.KEY, scanner.LPAREN)
		if err != nil {
			return false, err
		}

		primaryKeyPath, err := p.parsePath()
		if err != nil {
			return false, err
		}

		// Parse ")"
		err = p.parseTokens(scanner.RPAREN)
		if err != nil {
			return false, err
		}

		if pk := stmt.Info.GetPrimaryKey(); pk != nil {
			return false, stringutil.Errorf("table %q has more than one primary key", stmt.Info.TableName)
		}
		fc := stmt.Info.FieldConstraints.Get(primaryKeyPath)
		if fc == nil {
			err = stmt.Info.FieldConstraints.Add(&database.FieldConstraint{
				Path:         primaryKeyPath,
				IsPrimaryKey: true,
			})
			if err != nil {
				return false, err
			}
		} else {
			fc.IsPrimaryKey = true
		}

		return true, nil
	case scanner.UNIQUE:
		// Parse "("
		err = p.parseTokens(scanner.LPAREN)
		if err != nil {
			return false, err
		}

		uniquePath, err := p.parsePath()
		if err != nil {
			return false, err
		}

		// Parse ")"
		err = p.parseTokens(scanner.RPAREN)
		if err != nil {
			return false, err
		}

		fc := stmt.Info.FieldConstraints.Get(uniquePath)
		if fc == nil {
			err = stmt.Info.FieldConstraints.Add(&database.FieldConstraint{
				Path:     uniquePath,
				IsUnique: true,
			})
			if err != nil {
				return false, err
			}
		} else {
			fc.IsUnique = true
		}

		return true, nil
	default:
		p.Unscan()
		return false, nil
	}
}

// parseCreateIndexStatement parses a create index string and returns a Statement AST object.
// This function assumes the CREATE INDEX or CREATE UNIQUE INDEX tokens have already been consumed.
func (p *Parser) parseCreateIndexStatement(unique bool) (query.CreateIndexStmt, error) {
	var err error
	var stmt query.CreateIndexStmt
	stmt.Info.Unique = unique

	// Parse IF NOT EXISTS
	stmt.IfNotExists, err = p.parseOptional(scanner.IF, scanner.NOT, scanner.EXISTS)
	if err != nil {
		return stmt, err
	}

	// Parse optional index name
	stmt.Info.IndexName, err = p.parseIdent()
	if err != nil {
		// if IF NOT EXISTS is set, index name is mandatory
		if stmt.IfNotExists {
			return stmt, err
		}

		p.Unscan()
	}

	// Parse "ON"
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.ON {
		return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"ON"}, pos)
	}

	// Parse table name
	stmt.Info.TableName, err = p.parseIdent()
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

	stmt.Info.Paths = paths

	return stmt, nil
}
