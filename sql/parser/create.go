package parser

import (
	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/expr"
	"github.com/genjidb/genji/query"
	"github.com/genjidb/genji/sql/scanner"
	"github.com/genjidb/genji/stringutil"
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

	// parse field constraints
	err = p.parseConstraints(&stmt)
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

	if fc.Type == 0 && fc.DefaultValue.Type.IsZero() && !fc.IsNotNull && !fc.IsPrimaryKey {
		tok, pos, lit := p.ScanIgnoreWhitespace()
		return newParseError(scanner.Tokstr(tok, lit), []string{"TYPE", "CONSTRAINT"}, pos)
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
		tc, err := p.parseTableConstraint()
		if err != nil {
			return err
		}

		// no table constraint found
		if tc == nil && parsingTableConstraints {
			tok, pos, lit := p.ScanIgnoreWhitespace()
			return newParseError(scanner.Tokstr(tok, lit), []string{"CONSTRAINT", ")"}, pos)
		}

		// only PRIMARY KEY(path) is currently supported.
		if tc != nil {
			parsingTableConstraints = true

			if pk := stmt.Info.GetPrimaryKey(); pk != nil {
				return stringutil.Errorf("table %q has more than one primary key", stmt.TableName)
			}
			fc := stmt.Info.FieldConstraints.Get(tc.primaryKey)
			if fc == nil {
				err = stmt.Info.FieldConstraints.Add(&database.FieldConstraint{
					Path:         tc.primaryKey,
					IsPrimaryKey: true,
				})
				if err != nil {
					return err
				}
			} else {
				fc.IsPrimaryKey = true
			}
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
				return stringutil.Errorf("table %q has more than one primary key", stmt.TableName)
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
		default:
			p.Unscan()
			return nil
		}
	}
}

func (p *Parser) parseTableConstraint() (*tableConstraint, error) {
	var tc tableConstraint
	var err error

	tok, _, _ := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.PRIMARY:
		// Parse "KEY"
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.KEY {
			return nil, newParseError(scanner.Tokstr(tok, lit), []string{"KEY"}, pos)
		}

		// Parse "("
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.LPAREN {
			return nil, newParseError(scanner.Tokstr(tok, lit), []string{"("}, pos)
		}

		tc.primaryKey, err = p.parsePath()
		if err != nil {
			return nil, err
		}

		// Parse ")"
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.RPAREN {
			return nil, newParseError(scanner.Tokstr(tok, lit), []string{")"}, pos)
		}

		return &tc, nil
	default:
		p.Unscan()
		return nil, nil
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

type tableConstraint struct {
	primaryKey document.Path
}
