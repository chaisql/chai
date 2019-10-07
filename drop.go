package genji

import (
	"database/sql/driver"
	"errors"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/sql/scanner"
)

// parseDropStatement parses a drop string and returns a Statement AST object.
// This function assumes the DROP token has already been consumed.
func (p *Parser) parseDropStatement() (Statement, error) {
	tok, pos, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.TABLE:
		return p.parseDropTableStatement()
	case scanner.INDEX:
		return p.parseDropIndexStatement()
	}

	return nil, newParseError(scanner.Tokstr(tok, lit), []string{"TABLE", "INDEX"}, pos)
}

// parseDropTableStatement parses a drop table string and returns a Statement AST object.
// This function assumes the DROP TABLE tokens have already been consumed.
func (p *Parser) parseDropTableStatement() (dropTableStmt, error) {
	var stmt dropTableStmt
	var err error

	// Parse "IF"
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.IF {
		// Parse "EXISTS"
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.EXISTS {
			return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"EXISTS"}, pos)
		}
		stmt.ifExists = true
	} else {
		p.Unscan()
	}

	// Parse table name
	stmt.tableName, err = p.ParseIdent()
	if err != nil {
		return stmt, err
	}

	return stmt, nil
}

// dropTableStmt is a DSL that allows creating a DROP TABLE query.
type dropTableStmt struct {
	tableName string
	ifExists  bool
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt dropTableStmt) IsReadOnly() bool {
	return false
}

// Run runs the DropTable statement in the given transaction.
// It implements the Statement interface.
func (stmt dropTableStmt) Run(tx *database.Tx, args []driver.NamedValue) Result {
	if stmt.tableName == "" {
		return Result{err: errors.New("missing table name")}
	}

	err := tx.DropTable(stmt.tableName)
	if err == database.ErrTableNotFound && stmt.ifExists {
		return Result{}
	}

	return Result{err: err}
}

// parseDropIndexStatement parses a drop index string and returns a Statement AST object.
// This function assumes the DROP INDEX tokens have already been consumed.
func (p *Parser) parseDropIndexStatement() (dropIndexStmt, error) {
	var stmt dropIndexStmt
	var err error

	// Parse "IF"
	if tok, _, _ := p.ScanIgnoreWhitespace(); tok == scanner.IF {
		// Parse "EXISTS"
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.EXISTS {
			return stmt, newParseError(scanner.Tokstr(tok, lit), []string{"EXISTS"}, pos)
		}
		stmt.ifExists = true
	} else {
		p.Unscan()
	}

	// Parse index name
	stmt.indexName, err = p.ParseIdent()
	if err != nil {
		return stmt, err
	}

	return stmt, nil
}

// dropIndexStmt is a DSL that allows creating a DROP INDEX query.
type dropIndexStmt struct {
	indexName string
	ifExists  bool
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt dropIndexStmt) IsReadOnly() bool {
	return false
}

// Run runs the DropIndex statement in the given transaction.
// It implements the Statement interface.
func (stmt dropIndexStmt) Run(tx *database.Tx, args []driver.NamedValue) Result {
	if stmt.indexName == "" {
		return Result{err: errors.New("missing index name")}
	}

	err := tx.DropIndex(stmt.indexName)
	if err == database.ErrIndexNotFound && stmt.ifExists {
		err = nil
	}

	return Result{err: err}
}
