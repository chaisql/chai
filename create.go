package genji

import (
	"database/sql/driver"
	"errors"

	"github.com/asdine/genji/index"
	"github.com/asdine/genji/scanner"
)

// parseCreateStatement parses a create string and returns a Statement AST object.
// This function assumes the CREATE token has already been consumed.
func (p *Parser) parseCreateStatement() (Statement, error) {
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
func (p *Parser) parseCreateTableStatement() (createTableStmt, error) {
	var stmt createTableStmt
	var err error

	// Parse table name
	stmt.tableName, err = p.ParseIdent()
	if err != nil {
		return stmt, err
	}

	// Parse "IF"
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.IF {
		p.Unscan()
		return stmt, nil
	}

	// Parse "NOT"
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.NOT {
		return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"NOT", "EXISTS"}, pos)
	}

	// Parse "EXISTS"
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.EXISTS {
		return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"EXISTS"}, pos)
	}

	stmt.ifNotExists = true

	return stmt, nil
}

// parseCreateIndexStatement parses a create index string and returns a Statement AST object.
// This function assumes the CREATE INDEX or CREATE UNIQUE INDEX tokens have already been consumed.
func (p *Parser) parseCreateIndexStatement(unique bool) (createIndexStmt, error) {
	var err error
	stmt := createIndexStmt{
		unique: unique,
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

		stmt.ifNotExists = true
	} else {
		p.Unscan()
	}

	// Parse index name
	stmt.indexName, err = p.ParseIdent()
	if err != nil {
		return stmt, err
	}

	// Parse "ON"
	if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.ON {
		return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"ON"}, pos)
	}

	// Parse table name
	stmt.tableName, err = p.ParseIdent()
	if err != nil {
		return stmt, err
	}

	fields, ok, err := p.parseFieldList()
	if err != nil {
		return stmt, err
	}
	if !ok {
		tok, pos, lit := p.ScanIgnoreWhitespace()
		return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"("}, pos)
	}

	if len(fields) != 1 {
		return stmt, &ParseError{Message: "indexes on more than one field not supported"}
	}

	stmt.fieldName = fields[0]

	return stmt, nil
}

// createTableStmt is a DSL that allows creating a full CREATE TABLE statement.
type createTableStmt struct {
	tableName   string
	ifNotExists bool
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt createTableStmt) IsReadOnly() bool {
	return false
}

// Run runs the Create table statement in the given transaction.
// It implements the Statement interface.
func (stmt createTableStmt) Run(tx *Tx, args []driver.NamedValue) Result {
	if stmt.tableName == "" {
		return Result{err: errors.New("missing table name")}
	}

	var err error
	if stmt.ifNotExists {
		_, err = tx.CreateTableIfNotExists(stmt.tableName)
	} else {
		_, err = tx.CreateTable(stmt.tableName)
	}

	return Result{err: err}
}

// createIndexStmt is a DSL that allows creating a full CREATE INDEX statement.
// It is typically created using the CreateIndex function.
type createIndexStmt struct {
	indexName   string
	tableName   string
	fieldName   string
	ifNotExists bool
	unique      bool
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt createIndexStmt) IsReadOnly() bool {
	return false
}

// Run runs the Create table statement in the given transaction.
// It implements the Statement interface.
func (stmt createIndexStmt) Run(tx *Tx, args []driver.NamedValue) Result {
	if stmt.tableName == "" {
		return Result{err: errors.New("missing table name")}
	}

	if stmt.indexName == "" {
		return Result{err: errors.New("missing index name")}
	}

	if stmt.fieldName == "" {
		return Result{err: errors.New("missing field name")}
	}

	var err error

	if stmt.ifNotExists {
		_, err = tx.CreateIndexIfNotExists(stmt.indexName, stmt.tableName, stmt.fieldName, index.Options{Unique: stmt.unique})
	} else {
		_, err = tx.CreateIndex(stmt.indexName, stmt.tableName, stmt.fieldName, index.Options{Unique: stmt.unique})
	}

	return Result{err: err}
}
